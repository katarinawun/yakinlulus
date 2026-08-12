package cbt_runtime

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"yakinlulus.id/backend/internal/middleware"
	"yakinlulus.id/backend/internal/shared"
)

// --- Domain ---

type ExamSession struct {
	ID               uuid.UUID  `json:"id"`
	ExamID           uuid.UUID  `json:"exam_id"`
	UserID           uuid.UUID  `json:"user_id"`
	Status           string     `json:"status"`
	StartedAt        time.Time  `json:"started_at"`
	FinishedAt       *time.Time `json:"finished_at,omitempty"`
	RemainingSeconds *int       `json:"remaining_seconds,omitempty"`
	ViolationScore   int        `json:"violation_score"`
	IsTerminated     bool       `json:"is_terminated"`
	FinalScore       *float64   `json:"final_score,omitempty"`
}

type ExamAnswer struct {
	ID               uuid.UUID  `json:"id"`
	SessionID        uuid.UUID  `json:"session_id"`
	ExamQuestionID   uuid.UUID  `json:"exam_question_id"`
	SelectedOptionID *uuid.UUID `json:"selected_option_id,omitempty"`
	IsDoubtful       bool       `json:"is_doubtful"`
	IsCorrect        *bool      `json:"is_correct,omitempty"`
	PointsEarned     float64    `json:"points_earned"`
	QuestionContent  string     `json:"question_content,omitempty"`
	Difficulty       string     `json:"difficulty,omitempty"`
}

type Violation struct {
	ID            uuid.UUID `json:"id"`
	SessionID     uuid.UUID `json:"session_id"`
	ViolationType string    `json:"violation_type"`
	Details       string    `json:"details,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

type QuestionPool struct {
	ID                  uuid.UUID   `json:"id"`
	ExamID              uuid.UUID   `json:"exam_id"`
	SubjectID           uuid.UUID   `json:"subject_id"`
	ChapterIDs          []uuid.UUID `json:"chapter_ids,omitempty"`
	EasyPct             int         `json:"easy_pct"`
	MediumPct           int         `json:"medium_pct"`
	HardPct             int         `json:"hard_pct"`
	TotalPoolSize       int         `json:"total_pool_size"`
	QuestionsPerStudent int         `json:"questions_per_student"`
	ShuffleQuestions    bool        `json:"shuffle_questions"`
	ShuffleOptions      bool        `json:"shuffle_options"`
	CreatedAt           time.Time   `json:"created_at"`
	UpdatedAt           time.Time   `json:"updated_at"`
}

// --- Repository ---

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// cbtStatusFromLegacy maps a legacy ExamSession status to the
// cbt.exam_attempt.status CHECK value.
func cbtStatusFromLegacy(s string) string {
	switch s {
	case "PAUSED":
		return "PAUSED"
	case "FINISHED":
		return "COMPLETED"
	case "TERMINATED":
		return "SUBMITTED"
	default:
		return "STARTED"
	}
}

// legacyStatusFromCBT inverts cbtStatusFromLegacy for reads.
func legacyStatusFromCBT(s string) string {
	switch s {
	case "PAUSED":
		return "PAUSED"
	case "COMPLETED", "GRADING":
		return "FINISHED"
	case "SUBMITTED":
		return "TERMINATED"
	default:
		return "ACTIVE"
	}
}

// findOrCreateParticipant resolves (exam_id, student_id) to a
// cbt.exam_participant row, creating it (status REGISTER) when absent without
// resetting an existing row's state.
func (r *Repository) findOrCreateParticipant(ctx context.Context, examID, studentID uuid.UUID) (uuid.UUID, error) {
	var pid uuid.UUID
	err := r.pool.QueryRow(ctx, `
		INSERT INTO cbt.exam_participant (exam_id, student_id, status)
		VALUES ($1, $2, 'REGISTER')
		ON CONFLICT (exam_id, student_id) DO NOTHING
		RETURNING id`, examID, studentID).Scan(&pid)
	if err == pgx.ErrNoRows {
		if err := r.pool.QueryRow(ctx, `
			SELECT id FROM cbt.exam_participant WHERE exam_id = $1 AND student_id = $2`, examID, studentID).Scan(&pid); err != nil {
			return uuid.Nil, err
		}
		return pid, nil
	}
	if err != nil {
		return uuid.Nil, err
	}
	return pid, nil
}

// scanSession reads an ExamSession from cbt.exam_attempt + participant + timer
// + grading + cheating + auto_submit. condition (with $N placeholders) is
// appended to the WHERE clause; args are bound in order.
func (r *Repository) scanSession(ctx context.Context, condition string, args ...interface{}) (*ExamSession, error) {
	s := &ExamSession{}
	var cbtStatus string
	err := r.pool.QueryRow(ctx, `
		SELECT a.id, p.exam_id, p.student_id, a.status, a.started_at, a.finished_at,
		       t.remaining_second,
		       (SELECT COUNT(*) FROM cbt.cheating_log cl WHERE cl.attempt_id = a.id),
		       a.status = 'SUBMITTED'
		        OR EXISTS (SELECT 1 FROM cbt.auto_submit aus
		                   WHERE aus.attempt_id = a.id AND aus.reason = 'CHEATING'),
		       g.score
		FROM cbt.exam_attempt a
		JOIN cbt.exam_participant p ON p.id = a.participant_id
		LEFT JOIN cbt.exam_timer t ON t.attempt_id = a.id
		LEFT JOIN cbt.grading_result g ON g.attempt_id = a.id
		WHERE `+condition, args...).Scan(&s.ID, &s.ExamID, &s.UserID, &cbtStatus, &s.StartedAt, &s.FinishedAt,
		&s.RemainingSeconds, &s.ViolationScore, &s.IsTerminated, &s.FinalScore)
	if err != nil {
		return nil, err
	}
	if s.IsTerminated {
		s.Status = "TERMINATED"
	} else {
		s.Status = legacyStatusFromCBT(cbtStatus)
	}
	return s, nil
}

func (r *Repository) CreateSession(ctx context.Context, s *ExamSession, examDurationMinutes int) error {
	s.ID = uuid.New()
	s.Status = "ACTIVE"
	s.StartedAt = time.Now()
	rem := examDurationMinutes * 60
	s.RemainingSeconds = &rem
	s.ViolationScore = 0
	s.IsTerminated = false

	pid, err := r.findOrCreateParticipant(ctx, s.ExamID, s.UserID)
	if err != nil {
		return err
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var attemptNo int
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(attempt_no), 0) + 1 FROM cbt.exam_attempt WHERE participant_id = $1`, pid).Scan(&attemptNo); err != nil {
		return err
	}

	if _, err = tx.Exec(ctx, `
		INSERT INTO cbt.exam_attempt (id, participant_id, attempt_no, started_at, status)
		VALUES ($1, $2, $3, $4, $5)`,
		s.ID, pid, attemptNo, s.StartedAt, cbtStatusFromLegacy(s.Status)); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO cbt.exam_timer (attempt_id, remaining_second)
		VALUES ($1, $2)`, s.ID, *s.RemainingSeconds); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) FindSession(ctx context.Context, sessionID uuid.UUID) (*ExamSession, error) {
	return r.scanSession(ctx, `a.id = $1`, sessionID)
}

func (r *Repository) FindSessionByExamUser(ctx context.Context, examID, userID uuid.UUID) (*ExamSession, error) {
	return r.scanSession(ctx, `p.exam_id = $1 AND p.student_id = $2 AND a.status = 'STARTED'`, examID, userID)
}

func (r *Repository) ListUserSessions(ctx context.Context, userID uuid.UUID) ([]UserSessionSummary, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT a.id, p.exam_id, a.status, g.score, a.started_at,
		       EXISTS (SELECT 1 FROM cbt.auto_submit aus
		               WHERE aus.attempt_id = a.id AND aus.reason = 'CHEATING')
		FROM cbt.exam_attempt a
		JOIN cbt.exam_participant p ON p.id = a.participant_id
		LEFT JOIN cbt.grading_result g ON g.attempt_id = a.id
		WHERE p.student_id = $1
		ORDER BY a.started_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sessions []UserSessionSummary
	for rows.Next() {
		var s UserSessionSummary
		var cbtStatus string
		var cheated bool
		if err := rows.Scan(&s.ID, &s.ExamContentID, &cbtStatus, &s.Score, &s.StartedAt, &cheated); err != nil {
			return nil, err
		}
		if cheated {
			s.Status = "TERMINATED"
		} else {
			s.Status = legacyStatusFromCBT(cbtStatus)
		}
		sessions = append(sessions, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sessions, nil
}

type UserSessionSummary struct {
	ID            uuid.UUID `json:"id"`
	ExamContentID uuid.UUID `json:"exam_content_id"`
	Status        string    `json:"status"`
	Score         *float64  `json:"score,omitempty"`
	StartedAt     time.Time `json:"started_at"`
}

func (r *Repository) GetExamDuration(ctx context.Context, examID uuid.UUID) (int, error) {
	var dur *int
	err := r.pool.QueryRow(ctx, `SELECT duration_minute FROM cbt.exam_metadata WHERE exam_id = $1`, examID).Scan(&dur)
	if err == pgx.ErrNoRows || dur == nil {
		return 120, nil // default duration
	}
	if err != nil {
		return 120, err
	}
	return *dur, nil
}

func (r *Repository) GetExamQuestions(ctx context.Context, examID uuid.UUID) ([]struct {
	ID         uuid.UUID
	QuestionID uuid.UUID
	Order      int
}, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT epq.id, epq.question_id, epq.question_order
		FROM cbt.exam_package_question epq
		JOIN cbt.exam_package ep ON ep.id = epq.package_id
		WHERE ep.exam_id = $1
		ORDER BY epq.question_order`, examID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []struct {
		ID         uuid.UUID
		QuestionID uuid.UUID
		Order      int
	}
	for rows.Next() {
		var item struct {
			ID         uuid.UUID
			QuestionID uuid.UUID
			Order      int
		}
		if err := rows.Scan(&item.ID, &item.QuestionID, &item.Order); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// GetQuestionPool fetches the question pool config for an exam, aggregated
// from cbt.exam_question_pool difficulty-distribution rows. Returns nil,nil
// when the exam has no pool config (Start falls back to package questions).
func (r *Repository) GetQuestionPool(ctx context.Context, examID uuid.UUID) (*QuestionPool, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, chapter_id, subject_id, difficulty, total_question
		FROM cbt.exam_question_pool WHERE exam_id = $1`, examID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type pr struct {
		id         uuid.UUID
		chapter    *uuid.UUID
		subject    uuid.UUID
		difficulty string
		total      int
	}
	var prs []pr
	for rows.Next() {
		var p pr
		if err := rows.Scan(&p.id, &p.chapter, &p.subject, &p.difficulty, &p.total); err != nil {
			return nil, err
		}
		prs = append(prs, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(prs) == 0 {
		return nil, nil
	}

	qp := &QuestionPool{ExamID: examID, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	qp.ID = prs[0].id
	qp.SubjectID = prs[0].subject
	var easy, medium, hard int
	seen := map[uuid.UUID]bool{}
	for _, p := range prs {
		switch p.difficulty {
		case "EASY":
			easy += p.total
		case "MEDIUM":
			medium += p.total
		case "HARD":
			hard += p.total
		default:
			medium += p.total
		}
		if p.chapter != nil && *p.chapter != uuid.Nil && !seen[*p.chapter] {
			seen[*p.chapter] = true
			qp.ChapterIDs = append(qp.ChapterIDs, *p.chapter)
		}
	}
	total := easy + medium + hard
	qp.TotalPoolSize = total
	qp.QuestionsPerStudent = total
	if total > 0 {
		qp.EasyPct = easy * 100 / total
		qp.MediumPct = medium * 100 / total
		qp.HardPct = 100 - qp.EasyPct - qp.MediumPct
	}
	var shuffleQ, shuffleO *bool
	_ = r.pool.QueryRow(ctx, `
		SELECT random_question, random_option FROM cbt.exam_randomization WHERE exam_id = $1`, examID).Scan(&shuffleQ, &shuffleO)
	qp.ShuffleQuestions = shuffleQ != nil && *shuffleQ
	qp.ShuffleOptions = shuffleO != nil && *shuffleO
	return qp, nil
}

// SelectQuestionsForSession picks random published questions matching the
// pool's subject (+ chapter) and difficulty distribution.
func (r *Repository) SelectQuestionsForSession(ctx context.Context, pool *QuestionPool) ([]uuid.UUID, error) {
	args := []interface{}{pool.SubjectID}
	argN := 2
	from := `
		FROM question.question q
		JOIN question.question_metadata md ON md.question_id = q.id
		WHERE q.deleted_at IS NULL
		  AND q.status_id = (SELECT id FROM question.question_status WHERE code = 'PUBLISHED')
		  AND EXISTS (SELECT 1 FROM question.question_subject qs WHERE qs.question_id = q.id AND qs.subject_id = $1)`
	if len(pool.ChapterIDs) > 0 {
		from += fmt.Sprintf(` AND EXISTS (SELECT 1 FROM question.question_chapter qc WHERE qc.question_id = q.id AND qc.chapter_id = ANY($%d))`, argN)
		args = append(args, pool.ChapterIDs)
		argN++
	}

	easyCount := pool.TotalPoolSize * pool.EasyPct / 100
	mediumCount := pool.TotalPoolSize * pool.MediumPct / 100
	hardCount := pool.TotalPoolSize - easyCount - mediumCount

	buckets := []struct {
		diff  string
		count int
	}{
		{"EASY", easyCount}, {"MEDIUM", mediumCount}, {"HARD", hardCount},
	}
	collect := func(bucket struct {
		diff  string
		count int
	}) ([]uuid.UUID, error) {
		if bucket.count <= 0 {
			return nil, nil
		}
		rows, err := r.pool.Query(ctx, fmt.Sprintf(`SELECT q.id %s AND md.difficulty_level = $%d ORDER BY RANDOM() LIMIT $%d`, from, argN, argN+1),
			append(append([]interface{}{}, args...), bucket.diff, bucket.count)...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var ids []uuid.UUID
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
		return ids, rows.Err()
	}

	var allIDs []uuid.UUID
	for _, b := range buckets {
		ids, err := collect(b)
		if err != nil {
			return nil, err
		}
		allIDs = append(allIDs, ids...)
	}
	if len(allIDs) == 0 {
		return nil, nil
	}
	rand.Shuffle(len(allIDs), func(i, j int) {
		allIDs[i], allIDs[j] = allIDs[j], allIDs[i]
	})
	if pool.QuestionsPerStudent > 0 && pool.QuestionsPerStudent < len(allIDs) {
		allIDs = allIDs[:pool.QuestionsPerStudent]
	}
	return allIDs, nil
}

// AddSessionQuestions creates cbt.attempt_question (+ attempt_option labels)
// rows for an attempt.
func (r *Repository) AddSessionQuestions(ctx context.Context, sessionID uuid.UUID, questionIDs []uuid.UUID, shuffleQuestions, shuffleOptions bool) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for i, qid := range questionIDs {
		aqID := uuid.New()
		// Snapshot the question's current version_no so later grading resolves
		// correct-option labels against the version the student actually saw.
		var snapshotNo int
		if err := tx.QueryRow(ctx, `
			SELECT qv.version_no
			FROM question.question q
			JOIN question.question_version qv ON qv.id = q.current_version_id
			WHERE q.id = $1`, qid).Scan(&snapshotNo); err != nil {
			if err == pgx.ErrNoRows {
				snapshotNo = 1
			} else {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO cbt.attempt_question (id, attempt_id, question_id, display_order, snapshot_version)
			VALUES ($1, $2, $3, $4, $5)`, aqID, sessionID, qid, i+1, snapshotNo); err != nil {
			return err
		}

		order := "op.display_order"
		if shuffleOptions {
			order = "RANDOM()"
		}
		optRows, err := tx.Query(ctx, fmt.Sprintf(`
			SELECT op.label
			FROM question.question q
			JOIN question.question_option op ON op.question_version_id = q.current_version_id
			WHERE q.id = $1
			ORDER BY %s`, order), qid)
		if err != nil {
			return err
		}
		var labels []string
		for optRows.Next() {
			var l string
			if err := optRows.Scan(&l); err != nil {
				optRows.Close()
				return err
			}
			labels = append(labels, l)
		}
		optRows.Close()

		for j, l := range labels {
			if _, err := tx.Exec(ctx, `
				INSERT INTO cbt.attempt_option (attempt_question_id, option_label, display_order)
				VALUES ($1, $2, $3)`, aqID, l, j+1); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

// GetSessionQuestions returns the attempt's questions with assigned
// (shuffled) option ids resolved from attempt_option labels.
func (r *Repository) GetSessionQuestions(ctx context.Context, sessionID uuid.UUID) ([]struct {
	ExamQuestionID      uuid.UUID
	DisplayOrder        int
	AssignedOptionOrder []uuid.UUID
}, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT aq.id, aq.display_order,
		       COALESCE((
		         SELECT array_agg(op.id ORDER BY ao2.display_order)
		         FROM cbt.attempt_option ao2
		         JOIN question.question q2 ON q2.id = aq.question_id
		         JOIN question.question_option op ON op.question_version_id = q2.current_version_id
		                                              AND op.label = ao2.option_label
		         WHERE ao2.attempt_question_id = aq.id
		       ), '{}')
		FROM cbt.attempt_question aq
		WHERE aq.attempt_id = $1
		ORDER BY aq.display_order`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []struct {
		ExamQuestionID      uuid.UUID
		DisplayOrder        int
		AssignedOptionOrder []uuid.UUID
	}
	for rows.Next() {
		var item struct {
			ExamQuestionID      uuid.UUID
			DisplayOrder        int
			AssignedOptionOrder []uuid.UUID
		}
		if err := rows.Scan(&item.ExamQuestionID, &item.DisplayOrder, &item.AssignedOptionOrder); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// GetSessionQuestionsFull builds the full session question payload from
// cbt.attempt_question joined to question.question content (current version).
func (r *Repository) GetSessionQuestionsFull(ctx context.Context, sessionID uuid.UUID) ([]SessionQuestion, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT aq.id, aq.question_id, aq.display_order
		FROM cbt.attempt_question aq
		WHERE aq.attempt_id = $1
		ORDER BY aq.display_order`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type qrow struct {
		examQID      uuid.UUID
		contentID    uuid.UUID
		displayOrder int
	}
	var qrows []qrow
	for rows.Next() {
		var q qrow
		if err := rows.Scan(&q.examQID, &q.contentID, &q.displayOrder); err != nil {
			return nil, err
		}
		qrows = append(qrows, q)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var result []SessionQuestion
	for _, q := range qrows {
		sq := SessionQuestion{
			ExamQuestionID:    q.examQID,
			QuestionContentID: q.contentID,
			DisplayOrder:      q.displayOrder,
		}
		var qtype, diff string
		_ = r.pool.QueryRow(ctx, `
			SELECT COALESCE(q.question_type, 'SINGLE_CHOICE'),
			       COALESCE(md.difficulty_level, 'MEDIUM'),
			       COALESCE((SELECT s.name
			                 FROM question.question_subject qs
			                 JOIN academic.subject s ON s.id = qs.subject_id
			                 WHERE qs.question_id = q.id LIMIT 1), '')
			FROM question.question q
			LEFT JOIN question.question_metadata md ON md.question_id = q.id
			WHERE q.id = $1`, q.contentID).Scan(&qtype, &diff, &sq.SubjectName)
		sq.QuestionType = qtype
		sq.Difficulty = diff
		sq.Stimulus = ""
		sq.Stem = r.loadStem(ctx, q.contentID)

		optRows, err := r.pool.Query(ctx, `
			SELECT op.id, op.label,
			       COALESCE((SELECT string_agg(ob.content, '' ORDER BY ob.block_order)
			                  FROM question.option_block ob WHERE ob.option_id = op.id), '')
			FROM question.question q
			JOIN question.question_option op ON op.question_version_id = q.current_version_id
			WHERE q.id = $1
			ORDER BY op.display_order`, q.contentID)
		if err == nil {
			for optRows.Next() {
				var o SessionQuestionOption
				if err := optRows.Scan(&o.ID, &o.Label, &o.Text); err == nil {
					sq.Options = append(sq.Options, o)
				}
			}
			optRows.Close()
		}
		result = append(result, sq)
	}
	return result, nil
}

func (r *Repository) loadStem(ctx context.Context, questionID uuid.UUID) string {
	var stem string
	_ = r.pool.QueryRow(ctx, `
		SELECT COALESCE(string_agg(b.content, '' ORDER BY b.block_order), '')
		FROM question.question q
		JOIN question.question_block b ON b.question_version_id = q.current_version_id
		WHERE q.id = $1 AND b.block_type = 'PARAGRAPH'`, questionID).Scan(&stem)
	return stem
}

func (r *Repository) CheckExamStarted(ctx context.Context, examID uuid.UUID) (bool, error) {
	var count int
	err := r.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM cbt.exam_attempt a
		JOIN cbt.exam_participant p ON p.id = a.participant_id
		WHERE p.exam_id = $1 AND a.status = 'STARTED'`, examID).Scan(&count)
	return count > 0, err
}

// labelFromOptionID resolves an option uuid to its label, so answers can be
// stored by label. Reads the option row directly (its label is intrinsic to
// that option id regardless of version), propagating errors rather than
// swallowing a real selection.
func (r *Repository) labelFromOptionID(ctx context.Context, optID uuid.UUID) (string, error) {
	var label string
	err := r.pool.QueryRow(ctx, `SELECT label FROM question.question_option WHERE id = $1`, optID).Scan(&label)
	return label, err
}

// selectedLabelsFromIDs resolves a set of option uuids to their labels,
// preserving the caller's order. Multi-choice answers are stored comma-joined
// per question in cbt.student_answer.selected_option (kept within its
// varchar(10) width; covers up to five single-letter options).
func (r *Repository) selectedLabelsFromIDs(ctx context.Context, optIDs []uuid.UUID) ([]string, error) {
	labels := make([]string, 0, len(optIDs))
	for _, id := range optIDs {
		l, err := r.labelFromOptionID(ctx, id)
		if err != nil {
			return nil, err
		}
		labels = append(labels, l)
	}
	return labels, nil
}

func (r *Repository) SaveAnswer(ctx context.Context, attemptQuestionID uuid.UUID, labels []string, isDoubtful bool) error {
	selected := strings.Join(labels, ",")
	answeredAt := time.Now()

	if _, err := r.pool.Exec(ctx, `
		INSERT INTO cbt.student_answer (attempt_question_id, selected_option, answered_at)
		VALUES ($1, NULLIF($2, ''), $3)
		ON CONFLICT (attempt_question_id) DO UPDATE SET
			selected_option = NULLIF(EXCLUDED.selected_option, ''),
			answered_at = EXCLUDED.answered_at`,
		attemptQuestionID, selected, answeredAt); err != nil {
		return err
	}

	if isDoubtful {
		_, err := r.pool.Exec(ctx, `
			INSERT INTO cbt.bookmark_question (attempt_question_id)
			VALUES ($1)
			ON CONFLICT (attempt_question_id) DO NOTHING`, attemptQuestionID)
		return err
	}
	_, err := r.pool.Exec(ctx, `DELETE FROM cbt.bookmark_question WHERE attempt_question_id = $1`, attemptQuestionID)
	return err
}

func (r *Repository) GetAnswers(ctx context.Context, sessionID uuid.UUID) ([]ExamAnswer, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT aq.id, aq.attempt_id, aq.question_id, sa.selected_option, sa.answered_at,
		       EXISTS (SELECT 1 FROM cbt.bookmark_question bq WHERE bq.attempt_question_id = aq.id),
		       COALESCE(sa.selected_option IS NOT NULL AND
		         (SELECT ARRAY_AGG(op.label::text ORDER BY op.display_order)
		          FROM question.question_option op
		          JOIN question.question_version v ON v.id = op.question_version_id
		          WHERE v.question_id = aq.question_id AND v.version_no = aq.snapshot_version AND op.is_correct)
		           @> string_to_array(sa.selected_option, ',')
		          AND string_to_array(sa.selected_option, ',') @>
		         (SELECT ARRAY_AGG(op.label::text ORDER BY op.display_order)
		          FROM question.question_option op
		          JOIN question.question_version v ON v.id = op.question_version_id
		          WHERE v.question_id = aq.question_id AND v.version_no = aq.snapshot_version AND op.is_correct), false)
		FROM cbt.attempt_question aq
		LEFT JOIN cbt.student_answer sa ON sa.attempt_question_id = aq.id
		WHERE aq.attempt_id = $1
		ORDER BY aq.display_order`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var answers []ExamAnswer
	for rows.Next() {
		var a ExamAnswer
		var label *string
		var doubt, correct bool
		var answeredAt time.Time
		if err := rows.Scan(&a.ID, &a.SessionID, &a.ExamQuestionID, &label, &answeredAt, &doubt, &correct); err != nil {
			return nil, err
		}
		a.IsDoubtful = doubt
		if correct {
			c := true
			a.IsCorrect = &c
			a.PointsEarned = 1
		}
		if label != nil && *label != "" {
			var optID uuid.UUID
			_ = r.pool.QueryRow(ctx, `
				SELECT op.id FROM question.question q
				JOIN question.question_option op ON op.question_version_id = q.current_version_id
				WHERE q.id = (SELECT aq.question_id FROM cbt.attempt_question aq WHERE aq.id = $1)
				  AND op.label = $2`, a.ExamQuestionID, *label).Scan(&optID)
			a.SelectedOptionID = &optID
		}
		answers = append(answers, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return answers, nil
}

// GetSessionReview builds a SessionReview from cbt.grading_result + question
// content (current version).
func (r *Repository) GetSessionReview(ctx context.Context, sessionID uuid.UUID) (*SessionReview, error) {
	review := &SessionReview{SessionID: sessionID}
	err := r.pool.QueryRow(ctx, `
		SELECT p.exam_id, COALESCE(e.title, ''), p.student_id,
		       (SELECT COUNT(*) FROM cbt.attempt_question aq WHERE aq.attempt_id = a.id),
		       COALESCE(g.correct, 0), COALESCE(g.wrong, 0), COALESCE(g.blank, 0),
		       COALESCE(g.score, 0), COALESCE(md.passing_score, 0), COALESCE(g.passed, false),
		       COALESCE(EXTRACT(EPOCH FROM (a.finished_at - a.started_at))::int, 0),
		       COALESCE(g.created_at, a.created_at)
		FROM cbt.exam_attempt a
		JOIN cbt.exam_participant p ON p.id = a.participant_id
		JOIN cbt.exam e ON e.id = p.exam_id
		LEFT JOIN cbt.exam_metadata md ON md.exam_id = p.exam_id
		LEFT JOIN cbt.grading_result g ON g.attempt_id = a.id
		WHERE a.id = $1`, sessionID,
	).Scan(&review.ExamID, &review.ExamTitle, &review.UserID, &review.TotalQuestions,
		&review.CorrectCount, &review.WrongCount, &review.UnansweredCount, &review.Score,
		&review.PassingGrade, &review.IsPassed, &review.DurationSeconds, &review.CreatedAt)
	if err != nil {
		return nil, err
	}

	qRows, err := r.pool.Query(ctx, `
		SELECT aq.id, aq.question_id, aq.display_order
		FROM cbt.attempt_question aq
		WHERE aq.attempt_id = $1
		ORDER BY aq.display_order`, sessionID)
	if err != nil {
		return nil, err
	}
	defer qRows.Close()

	type qid struct {
		examQID      uuid.UUID
		contentID    uuid.UUID
		displayOrder int
	}
	var qids []qid
	for qRows.Next() {
		var q qid
		if err := qRows.Scan(&q.examQID, &q.contentID, &q.displayOrder); err != nil {
			return nil, err
		}
		qids = append(qids, q)
	}
	if err := qRows.Err(); err != nil {
		return nil, err
	}

	answers, _ := r.GetAnswers(ctx, sessionID)
	answersMap := make(map[uuid.UUID]ExamAnswer)
	for _, a := range answers {
		answersMap[a.ExamQuestionID] = a
	}

	for _, q := range qids {
		rq := ReviewQuestion{
			ExamQuestionID:    q.examQID,
			QuestionContentID: q.contentID,
			DisplayOrder:      q.displayOrder,
		}
		var qtype, diff, expl string
		_ = r.pool.QueryRow(ctx, `
			SELECT COALESCE(q.question_type, ''), COALESCE(md.difficulty_level, ''),
			       COALESCE((SELECT ex.content FROM question.explanation ex
			                  JOIN question.question_version v ON v.id = ex.question_version_id
			                  WHERE v.question_id = q.id LIMIT 1), '')
			FROM question.question q
			LEFT JOIN question.question_metadata md ON md.question_id = q.id
			WHERE q.id = $1`, q.contentID).Scan(&qtype, &diff, &expl)
		rq.QuestionType = qtype
		rq.Difficulty = diff
		rq.Explanation = expl

		optRows, err := r.pool.Query(ctx, `
			SELECT op.id, op.label,
			       COALESCE((SELECT string_agg(ob.content, '' ORDER BY ob.block_order)
			                  FROM question.option_block ob WHERE ob.option_id = op.id), ''),
			       op.is_correct
			FROM question.question q
			JOIN question.question_option op ON op.question_version_id = q.current_version_id
			WHERE q.id = $1
			ORDER BY op.display_order`, q.contentID)
		if err == nil {
			for optRows.Next() {
				var opt ReviewQuestionOption
				if err := optRows.Scan(&opt.ID, &opt.Label, &opt.Text, &opt.IsCorrect); err == nil {
					rq.Options = append(rq.Options, opt)
				}
			}
			optRows.Close()
		}

		if ans, ok := answersMap[q.examQID]; ok {
			rq.SelectedOptionID = ans.SelectedOptionID
			rq.IsCorrect = ans.IsCorrect
			rq.IsDoubtful = ans.IsDoubtful
		}
		review.Questions = append(review.Questions, rq)
	}
	return review, nil
}

func (r *Repository) GetCorrectOptionForQuestion(ctx context.Context, questionID uuid.UUID) (*uuid.UUID, error) {
	var optionID uuid.UUID
	err := r.pool.QueryRow(ctx, `
		SELECT op.id
		FROM question.question q
		JOIN question.question_option op ON op.question_version_id = q.current_version_id
		WHERE q.id = $1 AND op.is_correct
		ORDER BY op.display_order LIMIT 1`, questionID).Scan(&optionID)
	if err != nil {
		return nil, err
	}
	return &optionID, nil
}

func (r *Repository) GetCorrectOptionsForQuestion(ctx context.Context, questionID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT op.id
		FROM question.question q
		JOIN question.question_option op ON op.question_version_id = q.current_version_id
		WHERE q.id = $1 AND op.is_correct`, questionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var optionIDs []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		optionIDs = append(optionIDs, id)
	}
	return optionIDs, rows.Err()
}

func (r *Repository) GetQuestionType(ctx context.Context, questionID uuid.UUID) (string, error) {
	var qType string
	err := r.pool.QueryRow(ctx, `SELECT question_type FROM question.question WHERE id = $1`, questionID).Scan(&qType)
	if err != nil {
		return "", err
	}
	return qType, nil
}

func (r *Repository) GetQuestionIDByExamQuestion(ctx context.Context, examQuestionID uuid.UUID) (uuid.UUID, error) {
	var qID uuid.UUID
	err := r.pool.QueryRow(ctx, `SELECT question_id FROM cbt.attempt_question WHERE id = $1`, examQuestionID).Scan(&qID)
	return qID, err
}

func (r *Repository) FinishSession(ctx context.Context, sessionID uuid.UUID, remainingSeconds *int) error {
	now := time.Now()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		UPDATE cbt.exam_attempt
		SET status = CASE WHEN status = 'SUBMITTED' THEN 'SUBMITTED' ELSE 'COMPLETED' END,
		    finished_at = $1, last_sync = $1
		WHERE id = $2`, now, sessionID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO cbt.exam_timer (attempt_id, remaining_second)
		VALUES ($1, $2)
		ON CONFLICT (attempt_id) DO UPDATE SET remaining_second = EXCLUDED.remaining_second, last_update = NOW()`,
		sessionID, remainingSeconds); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) PauseSession(ctx context.Context, sessionID uuid.UUID, remainingSeconds int) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		UPDATE cbt.exam_attempt SET status = 'PAUSED', last_sync = NOW() WHERE id = $1`, sessionID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO cbt.exam_timer (attempt_id, remaining_second)
		VALUES ($1, $2)
		ON CONFLICT (attempt_id) DO UPDATE SET remaining_second = EXCLUDED.remaining_second, last_update = NOW()`,
		sessionID, remainingSeconds); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) ResumeSession(ctx context.Context, sessionID uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE cbt.exam_attempt SET status = 'STARTED', last_sync = NOW() WHERE id = $1`, sessionID)
	return err
}

// violationEvent maps a legacy violation type to a cbt.cheating_log CHECK value.
func violationEvent(v *Violation) string {
	switch v.ViolationType {
	case "TAB_CHANGE":
		return "TAB_CHANGE"
	case "COPY_ATTEMPT", "COPY":
		return "COPY"
	case "FULLSCREEN_EXIT", "KEYBOARD_SHORTCUT", "SUSPICIOUS_ACTIVITY":
		return "WINDOW_BLUR"
	case "DEVTOOLS_OPEN", "SCREENSHOT":
		return "SCREENSHOT"
	default:
		return "WINDOW_BLUR"
	}
}

func (r *Repository) SaveViolation(ctx context.Context, v *Violation) error {
	v.ID = uuid.New()
	v.CreatedAt = time.Now()
	_, err := r.pool.Exec(ctx, `
		INSERT INTO cbt.cheating_log (id, attempt_id, event, detail, created_at)
		VALUES ($1, $2, $3, to_jsonb($4::text), $5)`,
		v.ID, v.SessionID, violationEvent(v), v.Details, v.CreatedAt)
	return err
}

func (r *Repository) GetViolationScore(ctx context.Context, sessionID uuid.UUID) (int, error) {
	var score int
	err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM cbt.cheating_log WHERE attempt_id = $1`, sessionID).Scan(&score)
	return score, err
}

func (r *Repository) TerminateSession(ctx context.Context, sessionID uuid.UUID) error {
	now := time.Now()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		UPDATE cbt.exam_attempt SET status = 'SUBMITTED', finished_at = $1 WHERE id = $2`, now, sessionID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO cbt.auto_submit (attempt_id, reason)
		VALUES ($1, 'CHEATING')
		ON CONFLICT (attempt_id) DO NOTHING`, sessionID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) GetExamPassingScore(ctx context.Context, examID uuid.UUID) (float64, error) {
	var ps *float64
	err := r.pool.QueryRow(ctx, `SELECT passing_score FROM cbt.exam_metadata WHERE exam_id = $1`, examID).Scan(&ps)
	if err != nil || ps == nil {
		return 70.0, nil // default passing score
	}
	return *ps, nil
}

func (r *Repository) GetExamNegativeMarking(ctx context.Context, examID uuid.UUID) (float64, error) {
	var nm bool
	err := r.pool.QueryRow(ctx, `SELECT negative_marking FROM cbt.exam_metadata WHERE exam_id = $1`, examID).Scan(&nm)
	if err != nil || !nm {
		return 0.0, nil // default negative marking
	}
	return 0.25, nil
}

func (r *Repository) UpdateQuestionAnalytics(ctx context.Context, questionID uuid.UUID, correct, blank bool) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO cbt.question_statistics (question_id, shown_count, correct_count, wrong_count, blank_count, accuracy)
		VALUES ($1, 1, CASE WHEN $2 THEN 1 ELSE 0 END,
		        CASE WHEN NOT $2 AND NOT $3 THEN 1 ELSE 0 END,
		        CASE WHEN $3 THEN 1 ELSE 0 END,
		        CASE WHEN $2 THEN 100.0 ELSE 0.0 END)
		ON CONFLICT (question_id) DO UPDATE SET
			shown_count = cbt.question_statistics.shown_count + 1,
			correct_count = cbt.question_statistics.correct_count + CASE WHEN $2 THEN 1 ELSE 0 END,
			wrong_count = cbt.question_statistics.wrong_count + CASE WHEN NOT $2 AND NOT $3 THEN 1 ELSE 0 END,
			blank_count = cbt.question_statistics.blank_count + CASE WHEN $3 THEN 1 ELSE 0 END,
			accuracy = ROUND((100.0 * (cbt.question_statistics.correct_count + CASE WHEN $2 THEN 1 ELSE 0 END) /
			                     (cbt.question_statistics.shown_count + 1))::numeric, 2)`,
		questionID, correct, blank)
	return err
}

// SaveResult maps the Result DTO into cbt.grading_result + grading_detail.
func (r *Repository) SaveResult(ctx context.Context, res *Result) error {
	res.ID = uuid.New()
	res.CreatedAt = time.Now()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO cbt.grading_result (attempt_id, score, correct, wrong, blank, passed)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (attempt_id) DO UPDATE SET
			score = EXCLUDED.score, correct = EXCLUDED.correct, wrong = EXCLUDED.wrong,
			blank = EXCLUDED.blank, passed = EXCLUDED.passed`,
		res.SessionID, res.Score, res.CorrectCount, res.WrongCount, res.UnansweredCount, res.IsPassed); err != nil {
		return err
	}

	rows, err := tx.Query(ctx, `
		SELECT aq.id, aq.question_id,
		       COALESCE(sa.selected_option IS NOT NULL AND
		         (SELECT ARRAY_AGG(op.label::text ORDER BY op.display_order)
		          FROM question.question_option op
		          JOIN question.question_version v ON v.id = op.question_version_id
		          WHERE v.question_id = aq.question_id AND v.version_no = aq.snapshot_version AND op.is_correct)
		           @> string_to_array(sa.selected_option, ',')
		          AND string_to_array(sa.selected_option, ',') @>
		         (SELECT ARRAY_AGG(op.label::text ORDER BY op.display_order)
		          FROM question.question_option op
		          JOIN question.question_version v ON v.id = op.question_version_id
		          WHERE v.question_id = aq.question_id AND v.version_no = aq.snapshot_version AND op.is_correct), false),
		       sa.selected_option IS NULL
		FROM cbt.attempt_question aq
		LEFT JOIN cbt.student_answer sa ON sa.attempt_question_id = aq.id
		WHERE aq.attempt_id = $1`, res.SessionID)
	if err != nil {
		return err
	}
	type detail struct {
		aqID, qID uuid.UUID
		correct   bool
		blank     bool
	}
	var details []detail
	for rows.Next() {
		var d detail
		if err := rows.Scan(&d.aqID, &d.qID, &d.correct, &d.blank); err != nil {
			rows.Close()
			return err
		}
		details = append(details, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, d := range details {
		score := 0.0
		if d.correct && !d.blank {
			score = 1
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO cbt.grading_detail (attempt_question_id, question_id, status_correct, score, is_blank)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (attempt_question_id) DO UPDATE SET
				status_correct = EXCLUDED.status_correct, score = EXCLUDED.score, is_blank = EXCLUDED.is_blank`,
			d.aqID, d.qID, d.correct, score, d.blank); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (r *Repository) GetResult(ctx context.Context, sessionID uuid.UUID) (*Result, error) {
	rs := &Result{SessionID: sessionID}
	err := r.pool.QueryRow(ctx, `
		SELECT p.exam_id, p.student_id,
		       COALESCE(g.correct + g.wrong + g.blank, 0),
		       COALESCE(g.correct + g.wrong, 0),
		       COALESCE(g.correct, 0), COALESCE(g.wrong, 0), COALESCE(g.blank, 0),
		       COALESCE(g.score, 0), COALESCE(md.passing_score, 0), COALESCE(g.passed, false),
		       COALESCE(EXTRACT(EPOCH FROM (a.finished_at - a.started_at))::int, 0),
		       COALESCE(g.created_at, a.created_at)
		FROM cbt.exam_attempt a
		JOIN cbt.exam_participant p ON p.id = a.participant_id
		LEFT JOIN cbt.exam_metadata md ON md.exam_id = p.exam_id
		LEFT JOIN cbt.grading_result g ON g.attempt_id = a.id
		WHERE a.id = $1`, sessionID).Scan(&rs.ExamID, &rs.UserID, &rs.TotalQuestions,
		&rs.AnsweredCount, &rs.CorrectCount, &rs.WrongCount, &rs.UnansweredCount, &rs.Score,
		&rs.PassingGrade, &rs.IsPassed, &rs.DurationSeconds, &rs.CreatedAt)
	if err != nil {
		return nil, err
	}
	rs.ID = sessionID
	return rs, nil
}

func (r *Repository) UpdateSessionFinalScore(ctx context.Context, sessionID uuid.UUID, score float64) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO cbt.grading_result (attempt_id, score)
		VALUES ($1, $2)
		ON CONFLICT (attempt_id) DO UPDATE SET score = EXCLUDED.score`, sessionID, score)
	return err
}

func (r *Repository) GetQuestionsPerStudent(ctx context.Context, examID uuid.UUID) (int, error) {
	var total int
	err := r.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(total_question), 0) FROM cbt.exam_question_pool WHERE exam_id = $1`, examID).Scan(&total)
	return total, err
}

func (r *Repository) PickRandomExamQuestions(ctx context.Context, examID uuid.UUID, count int) ([]uuid.UUID, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT epq.question_id
		FROM cbt.exam_package_question epq
		JOIN cbt.exam_package ep ON ep.id = epq.package_id
		WHERE ep.exam_id = $1
		ORDER BY RANDOM() LIMIT $2`, examID, count)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// PickAllPackageQuestions returns every question authored for the exam
// (cbt.exam_package_question across its packages) in display order. Used as a
// fallback when an exam has no pool config / questions_per_student.
func (r *Repository) PickAllPackageQuestions(ctx context.Context, examID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT epq.question_id
		FROM cbt.exam_package_question epq
		JOIN cbt.exam_package ep ON ep.id = epq.package_id
		WHERE ep.exam_id = $1
		ORDER BY epq.question_order, epq.created_at`, examID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *Repository) FindExpiredActiveSessions(ctx context.Context) ([]struct {
	ID               uuid.UUID
	ExamID           uuid.UUID
	UserID           uuid.UUID
	RemainingSeconds *int
	ExamDuration     int
	StartTime        time.Time
}, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT a.id, p.exam_id, p.student_id, t.remaining_second, COALESCE(md.duration_minute, 1), a.started_at
		FROM cbt.exam_attempt a
		JOIN cbt.exam_participant p ON p.id = a.participant_id
		LEFT JOIN cbt.exam_timer t ON t.attempt_id = a.id
		LEFT JOIN cbt.exam_metadata md ON md.exam_id = p.exam_id
		WHERE a.status = 'STARTED'
		  AND (a.started_at + (COALESCE(md.duration_minute, 1) * interval '1 minute')) < NOW()`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []struct {
		ID               uuid.UUID
		ExamID           uuid.UUID
		UserID           uuid.UUID
		RemainingSeconds *int
		ExamDuration     int
		StartTime        time.Time
	}
	for rows.Next() {
		var item struct {
			ID               uuid.UUID
			ExamID           uuid.UUID
			UserID           uuid.UUID
			RemainingSeconds *int
			ExamDuration     int
			StartTime        time.Time
		}
		if err := rows.Scan(&item.ID, &item.ExamID, &item.UserID, &item.RemainingSeconds, &item.ExamDuration, &item.StartTime); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// --- Result DTO (shared with scoring module) ---

type Result struct {
	ID              uuid.UUID `json:"id"`
	SessionID       uuid.UUID `json:"session_id"`
	ExamID          uuid.UUID `json:"exam_id"`
	UserID          uuid.UUID `json:"user_id"`
	TotalQuestions  int       `json:"total_questions"`
	AnsweredCount   int       `json:"answered_count"`
	CorrectCount    int       `json:"correct_count"`
	WrongCount      int       `json:"wrong_count"`
	UnansweredCount int       `json:"unanswered_count"`
	Score           float64   `json:"score"`
	PassingGrade    float64   `json:"passing_grade"`
	IsPassed        bool      `json:"is_passed"`
	DurationSeconds int       `json:"duration_seconds"`
	CreatedAt       time.Time `json:"created_at"`
}

// --- Service ---

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) Start(ctx context.Context, examID, userID uuid.UUID) (*ExamSession, error) {
	// Check exam exists and is PUBLISHED (cbt.exam + cbt.exam_status).
	var status string
	err := s.repo.pool.QueryRow(ctx, `
		SELECT st.code
		FROM cbt.exam e
		LEFT JOIN cbt.exam_status st ON st.id = e.status_id
		WHERE e.id = $1 AND e.deleted_at IS NULL`, examID).Scan(&status)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fiber.NewError(404, "Exam not found")
		}
		return nil, err
	}
	if status != "PUBLISHED" {
		return nil, fiber.NewError(400, "Exam is not published")
	}

	// Check no active session for this user+exam
	existing, err := s.repo.FindSessionByExamUser(ctx, examID, userID)
	if err == nil && existing != nil {
		return existing, nil
	}

	dur, err := s.repo.GetExamDuration(ctx, examID)
	if err != nil {
		return nil, err
	}

	session := &ExamSession{
		ExamID: examID,
		UserID: userID,
	}

	if err := s.repo.CreateSession(ctx, session, dur); err != nil {
		return nil, err
	}

	// Check if exam has a question pool - if so, generate session questions
	addedQuestions := false
	pool, err := s.repo.GetQuestionPool(ctx, examID)
	if err == nil && pool != nil {
		questionIDs, err := s.repo.SelectQuestionsForSession(ctx, pool)
		if err != nil {
			return nil, err
		}
		if len(questionIDs) > 0 {
			err = s.repo.AddSessionQuestions(ctx, session.ID, questionIDs, pool.ShuffleQuestions, pool.ShuffleOptions)
			if err != nil {
				return nil, err
			}
			addedQuestions = true
		}
		return session, nil
	}

	// Fallback: pick random questions from package pool (questions_per_student)
	qps, bpErr := s.repo.GetQuestionsPerStudent(ctx, examID)
	if bpErr == nil && qps > 0 {
		questionIDs, qErr := s.repo.PickRandomExamQuestions(ctx, examID, qps)
		if qErr == nil && len(questionIDs) > 0 {
			if err := s.repo.AddSessionQuestions(ctx, session.ID, questionIDs, true, true); err != nil {
				return nil, err
			}
			addedQuestions = true
		}
	}

	// Final fallback: the exam has no pool config and no questions_per_student,
	// so serve the exam's full authored question set (cbt.exam_package_question).
	if !addedQuestions {
		ids, pErr := s.repo.PickAllPackageQuestions(ctx, examID)
		if pErr == nil && len(ids) > 0 {
			if err := s.repo.AddSessionQuestions(ctx, session.ID, ids, true, true); err != nil {
				return nil, err
			}
		}
	}

	return session, nil
}

func (s *Service) SyncAnswers(ctx context.Context, sessionID uuid.UUID, userID uuid.UUID, answers []SyncAnswerReq) error {
	session, err := s.repo.FindSession(ctx, sessionID)
	if err != nil {
		return fiber.NewError(404, "Session not found")
	}
	if session.UserID != userID {
		return fiber.NewError(403, "Not your session")
	}
	if session.Status != "ACTIVE" {
		return fiber.NewError(400, "Session is not active")
	}

	for _, a := range answers {
		eqID, _ := uuid.Parse(a.ExamQuestionID)

		var selectedOptionIDs []uuid.UUID
		if len(a.SelectedOptionIDs) > 0 {
			for _, sid := range a.SelectedOptionIDs {
				id, _ := uuid.Parse(sid)
				selectedOptionIDs = append(selectedOptionIDs, id)
			}
		} else if a.SelectedOptionID != nil {
			id, _ := uuid.Parse(*a.SelectedOptionID)
			selectedOptionIDs = append(selectedOptionIDs, id)
		}

		labels, err := s.repo.selectedLabelsFromIDs(ctx, selectedOptionIDs)
		if err != nil {
			return err
		}
		if err := s.repo.SaveAnswer(ctx, eqID, labels, a.IsDoubtful); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) Navigate(ctx context.Context, sessionID uuid.UUID, userID uuid.UUID, examQuestionID string, isDoubtful bool) error {
	session, err := s.repo.FindSession(ctx, sessionID)
	if err != nil {
		return fiber.NewError(404, "Session not found")
	}
	if session.UserID != userID {
		return fiber.NewError(403, "Not your session")
	}
	if session.Status != "ACTIVE" {
		return fiber.NewError(400, "Session is not active")
	}

	eqID, _ := uuid.Parse(examQuestionID)
	if err := s.repo.SaveAnswer(ctx, eqID, nil, isDoubtful); err != nil {
		return err
	}
	return nil
}

func (s *Service) GetAnswers(ctx context.Context, sessionID uuid.UUID, userID uuid.UUID) ([]ExamAnswer, error) {
	session, err := s.repo.FindSession(ctx, sessionID)
	if err != nil {
		return nil, fiber.NewError(404, "Session not found")
	}
	if session.UserID != userID {
		return nil, fiber.NewError(403, "Not your session")
	}

	answers, err := s.repo.GetAnswers(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	return answers, nil
}

func (s *Service) GetSessionReview(ctx context.Context, sessionID uuid.UUID, userID uuid.UUID) (*SessionReview, error) {
	session, err := s.repo.FindSession(ctx, sessionID)
	if err != nil {
		return nil, fiber.NewError(404, "Session not found")
	}
	if session.UserID != userID {
		return nil, fiber.NewError(403, "Not your session")
	}
	review, err := s.repo.GetSessionReview(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	return review, nil
}

func (s *Service) ListUserSessions(ctx context.Context, userID uuid.UUID) ([]UserSessionSummary, error) {
	return s.repo.ListUserSessions(ctx, userID)
}

func (s *Service) GetSessionQuestions(ctx context.Context, sessionID uuid.UUID, userID uuid.UUID) ([]SessionQuestion, error) {
	session, err := s.repo.FindSession(ctx, sessionID)
	if err != nil {
		return nil, fiber.NewError(404, "Session not found")
	}
	if session.UserID != userID {
		return nil, fiber.NewError(403, "Not your session")
	}
	return s.repo.GetSessionQuestionsFull(ctx, sessionID)
}

func (s *Service) Pause(ctx context.Context, sessionID uuid.UUID, userID uuid.UUID, remainingSeconds int) error {
	session, err := s.repo.FindSession(ctx, sessionID)
	if err != nil {
		return fiber.NewError(404, "Session not found")
	}
	if session.UserID != userID {
		return fiber.NewError(403, "Not your session")
	}
	if session.Status != "ACTIVE" {
		return fiber.NewError(400, "Session is not active")
	}
	return s.repo.PauseSession(ctx, sessionID, remainingSeconds)
}

func (s *Service) Resume(ctx context.Context, sessionID uuid.UUID, userID uuid.UUID) error {
	session, err := s.repo.FindSession(ctx, sessionID)
	if err != nil {
		return fiber.NewError(404, "Session not found")
	}
	if session.UserID != userID {
		return fiber.NewError(403, "Not your session")
	}
	if session.Status != "PAUSED" {
		return fiber.NewError(400, "Session is not paused")
	}
	return s.repo.ResumeSession(ctx, sessionID)
}

func (s *Service) ReportViolation(ctx context.Context, sessionID uuid.UUID, userID uuid.UUID, violationType, details string) (*ExamSession, error) {
	session, err := s.repo.FindSession(ctx, sessionID)
	if err != nil {
		return nil, fiber.NewError(404, "Session not found")
	}
	if session.UserID != userID {
		return nil, fiber.NewError(403, "Not your session")
	}

	v := &Violation{
		SessionID:     sessionID,
		ViolationType: violationType,
		Details:       details,
	}
	if err := s.repo.SaveViolation(ctx, v); err != nil {
		return nil, err
	}

	score, err := s.repo.GetViolationScore(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	if score >= 5 {
		if err := s.repo.TerminateSession(ctx, sessionID); err != nil {
			return nil, err
		}
		if _, err := s.Finish(ctx, sessionID, userID); err != nil {
			// don't fail on scoring error
		}
		session.Status = "TERMINATED"
		session.IsTerminated = true
	} else {
		session.ViolationScore = score
	}

	return session, nil
}

func (s *Service) Finish(ctx context.Context, sessionID uuid.UUID, userID uuid.UUID) (*Result, error) {
	session, err := s.repo.FindSession(ctx, sessionID)
	if err != nil {
		return nil, fiber.NewError(404, "Session not found")
	}
	if session.UserID != userID {
		return nil, fiber.NewError(403, "Not your session")
	}
	if session.Status == "FINISHED" || session.Status == "TERMINATED" {
		// Already finished, return existing result if available
		existingRes, err := s.repo.GetResult(ctx, sessionID)
		if err == nil && existingRes != nil {
			return existingRes, nil
		}
	}

	// Calculate stats
	answers, err := s.repo.GetAnswers(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	// Use session-specific questions if available, fall back to exam questions
	sessionQuestions, err := s.repo.GetSessionQuestions(ctx, sessionID)
	if err != nil || len(sessionQuestions) == 0 {
		// Fallback to exam questions
		examQuestions, err := s.repo.GetExamQuestions(ctx, session.ExamID)
		if err != nil {
			return nil, err
		}
		totalQuestions := len(examQuestions)
		answeredCount := 0
		correctCount := 0
		for _, a := range answers {
			if a.SelectedOptionID != nil {
				answeredCount++
			}
			if a.IsCorrect != nil && *a.IsCorrect {
				correctCount++
			}
		}
		wrongCount := answeredCount - correctCount
		unansweredCount := totalQuestions - answeredCount

		negMarking, err := s.repo.GetExamNegativeMarking(ctx, session.ExamID)
		if err != nil {
			return nil, err
		}

		var score float64
		if totalQuestions > 0 {
			score = (float64(correctCount) - float64(wrongCount)*negMarking) / float64(totalQuestions) * 100
			if score < 0 {
				score = 0
			}
		}

		passingGrade, err := s.repo.GetExamPassingScore(ctx, session.ExamID)
		if err != nil {
			return nil, err
		}

		isPassed := score >= passingGrade

		for _, a := range answers {
			qid, err := s.repo.GetQuestionIDByExamQuestion(ctx, a.ExamQuestionID)
			if err != nil {
				continue
			}
			if err := s.repo.UpdateQuestionAnalytics(ctx, qid, a.IsCorrect != nil && *a.IsCorrect, a.SelectedOptionID == nil); err != nil {
			continue
		}
		}

		durationSeconds := int(time.Since(session.StartedAt).Seconds())
		if session.FinishedAt != nil {
			durationSeconds = int(session.FinishedAt.Sub(session.StartedAt).Seconds())
		}

		var remainingSec *int
		if session.RemainingSeconds != nil {
			elapsed := int(time.Since(session.StartedAt).Seconds())
			rem := *session.RemainingSeconds - elapsed
			if rem < 0 {
				rem = 0
			}
			remainingSec = &rem
		}

		if err := s.repo.FinishSession(ctx, sessionID, remainingSec); err != nil {
			return nil, err
		}

		result := &Result{
			SessionID:       sessionID,
			ExamID:          session.ExamID,
			UserID:          session.UserID,
			TotalQuestions:  totalQuestions,
			AnsweredCount:   answeredCount,
			CorrectCount:    correctCount,
			WrongCount:      wrongCount,
			UnansweredCount: unansweredCount,
			Score:           score,
			PassingGrade:    passingGrade,
			IsPassed:        isPassed,
			DurationSeconds: durationSeconds,
		}

		if err := s.repo.SaveResult(ctx, result); err != nil {
			return nil, err
		}

		if err := s.repo.UpdateSessionFinalScore(ctx, sessionID, score); err != nil {
			return nil, err
		}

		return result, nil
	}

	// Use session questions
	totalQuestions := len(sessionQuestions)
	answeredCount := 0
	correctCount := 0
	for _, a := range answers {
		if a.SelectedOptionID != nil {
			answeredCount++
		}
		if a.IsCorrect != nil && *a.IsCorrect {
			correctCount++
		}
	}
	wrongCount := answeredCount - correctCount
	unansweredCount := totalQuestions - answeredCount

	negMarking, err := s.repo.GetExamNegativeMarking(ctx, session.ExamID)
	if err != nil {
		return nil, err
	}

	var score float64
	if totalQuestions > 0 {
		score = (float64(correctCount) - float64(wrongCount)*negMarking) / float64(totalQuestions) * 100
		if score < 0 {
			score = 0
		}
	}

	passingGrade, err := s.repo.GetExamPassingScore(ctx, session.ExamID)
	if err != nil {
		return nil, err
	}

	isPassed := score >= passingGrade

	for _, a := range answers {
		qid, err := s.repo.GetQuestionIDByExamQuestion(ctx, a.ExamQuestionID)
		if err != nil {
			continue
		}
if err := s.repo.UpdateQuestionAnalytics(ctx, qid, a.IsCorrect != nil && *a.IsCorrect, a.SelectedOptionID == nil); err != nil {
			continue
		}
	}

	durationSeconds := int(time.Since(session.StartedAt).Seconds())
	if session.FinishedAt != nil {
		durationSeconds = int(session.FinishedAt.Sub(session.StartedAt).Seconds())
	}

	var remainingSec *int
	if session.RemainingSeconds != nil {
		elapsed := int(time.Since(session.StartedAt).Seconds())
		rem := *session.RemainingSeconds - elapsed
		if rem < 0 {
			rem = 0
		}
		remainingSec = &rem
	}

	if err := s.repo.FinishSession(ctx, sessionID, remainingSec); err != nil {
		return nil, err
	}

	result := &Result{
		SessionID:       sessionID,
		ExamID:          session.ExamID,
		UserID:          session.UserID,
		TotalQuestions:  totalQuestions,
		AnsweredCount:   answeredCount,
		CorrectCount:    correctCount,
		WrongCount:      wrongCount,
		UnansweredCount: unansweredCount,
		Score:           score,
		PassingGrade:    passingGrade,
		IsPassed:        isPassed,
		DurationSeconds: durationSeconds,
	}

	if err := s.repo.SaveResult(ctx, result); err != nil {
		return nil, err
	}

	if err := s.repo.UpdateSessionFinalScore(ctx, sessionID, score); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *Service) AutoSubmitExpired(ctx context.Context) (int, error) {
	sessions, err := s.repo.FindExpiredActiveSessions(ctx)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, sess := range sessions {
		if _, err := s.Finish(ctx, sess.ID, sess.UserID); err == nil {
			count++
		}
	}
	return count, nil
}

// --- DTOs ---

type SessionQuestionOption struct {
	ID    uuid.UUID `json:"id"`
	Label string    `json:"label"`
	Text  string    `json:"text"`
}

type SessionQuestion struct {
	ExamQuestionID    uuid.UUID               `json:"exam_question_id"`
	QuestionContentID uuid.UUID               `json:"question_content_id"`
	DisplayOrder      int                     `json:"display_order"`
	SubjectName       string                  `json:"subjectName"`
	Stimulus          string                  `json:"stimulus"`
	Stem              string                  `json:"stem"`
	QuestionType      string                  `json:"questionType"`
	Difficulty        string                  `json:"difficulty"`
	Options           []SessionQuestionOption `json:"options"`
}

type ReviewQuestionOption struct {
	ID        uuid.UUID `json:"id"`
	Label     string    `json:"label"`
	Text      string    `json:"text"`
	IsCorrect bool      `json:"is_correct"`
}

type ReviewQuestion struct {
	ExamQuestionID    uuid.UUID              `json:"exam_question_id"`
	QuestionContentID uuid.UUID              `json:"question_content_id"`
	DisplayOrder      int                    `json:"display_order"`
	Stem              string                 `json:"stem"`
	QuestionType      string                 `json:"question_type"`
	Difficulty        string                 `json:"difficulty"`
	Explanation       string                 `json:"explanation"`
	Options           []ReviewQuestionOption `json:"options"`
	SelectedOptionID  *uuid.UUID             `json:"selected_option_id,omitempty"`
	IsCorrect         *bool                  `json:"is_correct,omitempty"`
	IsDoubtful        bool                   `json:"is_doubtful"`
}

type SessionReview struct {
	SessionID       uuid.UUID        `json:"session_id"`
	ExamID          uuid.UUID        `json:"exam_id"`
	ExamTitle       string           `json:"exam_title"`
	UserID          uuid.UUID        `json:"user_id"`
	TotalQuestions  int              `json:"total_questions"`
	CorrectCount    int              `json:"correct_count"`
	WrongCount      int              `json:"wrong_count"`
	UnansweredCount int              `json:"unanswered_count"`
	Score           float64          `json:"score"`
	PassingGrade    float64          `json:"passing_grade"`
	IsPassed        bool             `json:"is_passed"`
	DurationSeconds int              `json:"duration_seconds"`
	Questions       []ReviewQuestion `json:"questions"`
	CreatedAt       time.Time        `json:"created_at"`
}

type SyncAnswerReq struct {
	ExamQuestionID    string   `json:"exam_question_id"`
	SelectedOptionIDs []string `json:"selected_option_ids,omitempty"` // For MULTIPLE_CHOICE
	SelectedOptionID  *string  `json:"selected_option_id,omitempty"`  // For SINGLE_CHOICE, TRUE_FALSE
	IsDoubtful        bool     `json:"is_doubtful"`
}

type ViolationReq struct {
	ViolationType string `json:"violation_type"`
	Details       string `json:"details,omitempty"`
}

type NavigateReq struct {
	ExamQuestionID string `json:"exam_question_id"`
	IsDoubtful     bool   `json:"is_doubtful"`
}

type PauseReq struct {
	RemainingSeconds int `json:"remaining_seconds"`
}

// --- Handler ---

type Handler struct {
	svc *Service
	jwt string
}

func NewHandler(svc *Service, jwtSecret string) *Handler {
	return &Handler{svc: svc, jwt: jwtSecret}
}

func (h *Handler) RegisterRoutes(router fiber.Router) {
	r := router.Group("/cbt", middleware.RequireAuth(h.jwt))
	r.Get("/sessions", h.ListSessions)
	r.Post("/:exam_id/start", h.Start)
	r.Post("/:session_id/sync", h.Sync)
	r.Post("/:session_id/navigate", h.Navigate)
	r.Post("/:session_id/pause", h.Pause)
	r.Post("/:session_id/resume", h.Resume)
	r.Post("/:session_id/finish", h.Finish)
	r.Post("/:session_id/violation", h.ReportViolation)
	r.Get("/:session_id/answers", h.GetAnswers)
	r.Get("/:session_id/questions", h.GetSessionQuestions)
	r.Get("/:session_id/review", h.GetReview)
}

func (h *Handler) AutoSubmitExpired(c *fiber.Ctx) error {
	count, err := h.svc.AutoSubmitExpired(c.Context())
	if err != nil {
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to auto-submit expired sessions: "+err.Error()))
	}
	return c.JSON(shared.Success(fiber.Map{"auto_submitted": count}))
}

func (h *Handler) Start(c *fiber.Ctx) error {
	examID, err := uuid.Parse(c.Params("exam_id"))
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid exam ID"))
	}
	userID := uuid.MustParse(c.Locals("user_id").(string))

	session, err := h.svc.Start(c.Context(), examID, userID)
	if err != nil {
		if fe, ok := err.(*fiber.Error); ok {
			return c.Status(fe.Code).JSON(shared.Error(shared.ErrorCode(fe.Message), fe.Message))
		}
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to start exam: "+err.Error()))
	}
	return c.Status(201).JSON(shared.Success(session))
}

func (h *Handler) Sync(c *fiber.Ctx) error {
	sessionID, err := uuid.Parse(c.Params("session_id"))
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid session ID"))
	}

	var req struct {
		Answers []SyncAnswerReq `json:"answers"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid request body"))
	}
	if len(req.Answers) == 0 {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "answers required"))
	}

	userID, ok := middleware.UserIDFromCtx(c)
	if !ok {
		return c.Status(401).JSON(shared.Error(shared.ErrUnauthorized, "Unauthorized"))
	}
	if err := h.svc.SyncAnswers(c.Context(), sessionID, userID, req.Answers); err != nil {
		if fe, ok := err.(*fiber.Error); ok {
			return c.Status(fe.Code).JSON(shared.Error(shared.ErrorCode(fe.Message), fe.Message))
		}
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to sync answers"))
	}
	return c.JSON(shared.Success(fiber.Map{"message": "Answers saved"}))
}

func (h *Handler) Navigate(c *fiber.Ctx) error {
	sessionID, err := uuid.Parse(c.Params("session_id"))
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid session ID"))
	}

	var req NavigateReq
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid request body"))
	}

	userID, ok := middleware.UserIDFromCtx(c)
	if !ok {
		return c.Status(401).JSON(shared.Error(shared.ErrUnauthorized, "Unauthorized"))
	}
	if err := h.svc.Navigate(c.Context(), sessionID, userID, req.ExamQuestionID, req.IsDoubtful); err != nil {
		if fe, ok := err.(*fiber.Error); ok {
			return c.Status(fe.Code).JSON(shared.Error(shared.ErrorCode(fe.Message), fe.Message))
		}
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to navigate"))
	}
	return c.JSON(shared.Success(fiber.Map{"message": "Doubtful flag updated"}))
}

func (h *Handler) Pause(c *fiber.Ctx) error {
	sessionID, err := uuid.Parse(c.Params("session_id"))
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid session ID"))
	}

	var req PauseReq
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid request body"))
	}

	userID, ok := middleware.UserIDFromCtx(c)
	if !ok {
		return c.Status(401).JSON(shared.Error(shared.ErrUnauthorized, "Unauthorized"))
	}
	if err := h.svc.Pause(c.Context(), sessionID, userID, req.RemainingSeconds); err != nil {
		if fe, ok := err.(*fiber.Error); ok {
			return c.Status(fe.Code).JSON(shared.Error(shared.ErrorCode(fe.Message), fe.Message))
		}
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to pause session"))
	}
	return c.JSON(shared.Success(fiber.Map{"message": "Session paused"}))
}

func (h *Handler) Resume(c *fiber.Ctx) error {
	sessionID, err := uuid.Parse(c.Params("session_id"))
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid session ID"))
	}

	userID, ok := middleware.UserIDFromCtx(c)
	if !ok {
		return c.Status(401).JSON(shared.Error(shared.ErrUnauthorized, "Unauthorized"))
	}
	if err := h.svc.Resume(c.Context(), sessionID, userID); err != nil {
		if fe, ok := err.(*fiber.Error); ok {
			return c.Status(fe.Code).JSON(shared.Error(shared.ErrorCode(fe.Message), fe.Message))
		}
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to resume session"))
	}
	return c.JSON(shared.Success(fiber.Map{"message": "Session resumed"}))
}

func (h *Handler) Finish(c *fiber.Ctx) error {
	sessionID, err := uuid.Parse(c.Params("session_id"))
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid session ID"))
	}

	userID, ok := middleware.UserIDFromCtx(c)
	if !ok {
		return c.Status(401).JSON(shared.Error(shared.ErrUnauthorized, "Unauthorized"))
	}
	result, err := h.svc.Finish(c.Context(), sessionID, userID)
	if err != nil {
		if fe, ok := err.(*fiber.Error); ok {
			return c.Status(fe.Code).JSON(shared.Error(shared.ErrorCode(fe.Message), fe.Message))
		}
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to finish exam"))
	}
	return c.JSON(shared.Success(result))
}

func (h *Handler) ReportViolation(c *fiber.Ctx) error {
	sessionID, err := uuid.Parse(c.Params("session_id"))
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid session ID"))
	}

	var req ViolationReq
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid request body"))
	}

	validTypes := map[string]bool{
		"FULLSCREEN_EXIT": true, "TAB_SWITCH": true, "KEYBOARD_SHORTCUT": true,
		"DEVTOOLS_OPEN": true, "COPY_ATTEMPT": true, "MULTIPLE_IP": true, "SUSPICIOUS_ACTIVITY": true,
	}
	if !validTypes[req.ViolationType] {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid violation type"))
	}

	userID, ok := middleware.UserIDFromCtx(c)
	if !ok {
		return c.Status(401).JSON(shared.Error(shared.ErrUnauthorized, "Unauthorized"))
	}
	session, err := h.svc.ReportViolation(c.Context(), sessionID, userID, req.ViolationType, req.Details)
	if err != nil {
		if fe, ok := err.(*fiber.Error); ok {
			return c.Status(fe.Code).JSON(shared.Error(shared.ErrorCode(fe.Message), fe.Message))
		}
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to report violation"))
	}
	return c.JSON(shared.Success(session))
}

func (h *Handler) GetAnswers(c *fiber.Ctx) error {
	sessionID, err := uuid.Parse(c.Params("session_id"))
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid session ID"))
	}

	userID, ok := middleware.UserIDFromCtx(c)
	if !ok {
		return c.Status(401).JSON(shared.Error(shared.ErrUnauthorized, "Unauthorized"))
	}
	answers, err := h.svc.GetAnswers(c.Context(), sessionID, userID)
	if err != nil {
		if fe, ok := err.(*fiber.Error); ok {
			return c.Status(fe.Code).JSON(shared.Error(shared.ErrorCode(fe.Message), fe.Message))
		}
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to get answers"))
	}
	return c.JSON(shared.Success(answers))
}

func (h *Handler) GetSessionQuestions(c *fiber.Ctx) error {
	sessionID, err := uuid.Parse(c.Params("session_id"))
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid session ID"))
	}

	userID, ok := middleware.UserIDFromCtx(c)
	if !ok {
		return c.Status(401).JSON(shared.Error(shared.ErrUnauthorized, "Unauthorized"))
	}
	questions, err := h.svc.GetSessionQuestions(c.Context(), sessionID, userID)
	if err != nil {
		if fe, ok := err.(*fiber.Error); ok {
			return c.Status(fe.Code).JSON(shared.Error(shared.ErrorCode(fe.Message), fe.Message))
		}
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to get session questions"))
	}
	return c.JSON(shared.Success(questions))
}

func (h *Handler) GetReview(c *fiber.Ctx) error {
	sessionID, err := uuid.Parse(c.Params("session_id"))
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid session ID"))
	}

	userID, ok := middleware.UserIDFromCtx(c)
	if !ok {
		return c.Status(401).JSON(shared.Error(shared.ErrUnauthorized, "Unauthorized"))
	}
	review, err := h.svc.GetSessionReview(c.Context(), sessionID, userID)
	if err != nil {
		if fe, ok := err.(*fiber.Error); ok {
			return c.Status(fe.Code).JSON(shared.Error(shared.ErrorCode(fe.Message), fe.Message))
		}
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to get review"))
	}
	if review == nil || review.UserID != userID {
		return c.Status(404).JSON(shared.Error(shared.ErrNotFound, "Session not found"))
	}
	return c.JSON(shared.Success(review))
}

func (h *Handler) ListSessions(c *fiber.Ctx) error {
	userID := uuid.MustParse(c.Locals("user_id").(string))
	sessions, err := h.svc.ListUserSessions(c.Context(), userID)
	if err != nil {
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to list sessions"))
	}
	return c.JSON(shared.Success(sessions))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}

