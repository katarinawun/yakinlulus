package content

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var examCustomBlueprintStore sync.Map

type repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) Repository {
	return &repository{pool: pool}
}

// ========== BASE CONTENT ==========

func (r *repository) CreateContent(ctx context.Context, c *Content) error {
	if c.ID == uuid.Nil {
		c.ID = uuid.New()
	}
	c.CreatedAt = time.Now()
	c.UpdatedAt = time.Now()
	if c.Status == "" {
		c.Status = StatusDraft
	}
	if c.Metadata == nil {
		c.Metadata = map[string]interface{}{}
	}

	// MATERIAL masters live in content.material; EXAM masters live in cbt.exam;
	// QUESTION (and other non-material/non-exam) content masters live in
	// question.question. All three live in the new schema.
	if c.ContentType == ContentTypeMaterial {
		return r.createMaterialContent(ctx, c)
	}
	if c.ContentType == ContentTypeExam {
		return r.createExamContent(ctx, c)
	}
	return r.createQuestionContent(ctx, c)
}

// ========== MATERIAL SCHEMA HELPERS (content.material) ==========

const materialStatusCodes = `('DRAFT','Draft'),('REVIEW','In Review'),('APPROVED','Approved'),('PUBLISHED','Published'),('ARCHIVED','Archived')`

func (r *repository) ensureMaterialStatuses(ctx context.Context, tx pgx.Tx) (map[string]uuid.UUID, error) {
	statuses := map[string]uuid.UUID{}
	if _, err := tx.Exec(ctx, `
		INSERT INTO content.material_status (code, name) VALUES `+materialStatusCodes+`
		ON CONFLICT (code) DO NOTHING`); err != nil {
		return nil, err
	}
	rws, err := tx.Query(ctx, `SELECT code, id FROM content.material_status WHERE code = ANY($1)`,
		[]string{"DRAFT", "REVIEW", "APPROVED", "PUBLISHED", "ARCHIVED"})
	if err != nil {
		return nil, err
	}
	defer rws.Close()
	for rws.Next() {
		var code string
		var id uuid.UUID
		if err := rws.Scan(&code, &id); err != nil {
			return nil, err
		}
		statuses[code] = id
	}
	return statuses, rws.Err()
}

func (r *repository) ensureMaterialTypes(ctx context.Context, tx pgx.Tx) (map[string]uuid.UUID, error) {
	rows := []string{"TEXT", "RICH_TEXT", "MARKDOWN", "VIDEO", "PDF", "AUDIO", "INTERACTIVE"}
	for _, c := range rows {
		if _, err := tx.Exec(ctx, `
			INSERT INTO content.material_type (code, name) VALUES ($1, $2)
			ON CONFLICT (code) DO NOTHING`, c, c); err != nil {
			return nil, err
		}
	}
	types := map[string]uuid.UUID{}
	rws, err := tx.Query(ctx, `SELECT code, id FROM content.material_type WHERE code = ANY($1)`, rows)
	if err != nil {
		return nil, err
	}
	defer rws.Close()
	for rws.Next() {
		var code string
		var id uuid.UUID
		if err := rws.Scan(&code, &id); err != nil {
			return nil, err
		}
		types[code] = id
	}
	return types, rws.Err()
}

// linkMaterialJunction inserts an N:M row only when the referenced academic
// row exists, so an empty academic catalog degrades gracefully.
func (r *repository) linkMaterialJunction(ctx context.Context, tx pgx.Tx, junction, refTable, refCol string, materialID, refID uuid.UUID) error {
	if refID == uuid.Nil {
		return nil
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+refTable+` WHERE id = $1)`, refID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	_, err := tx.Exec(ctx, `INSERT INTO content.`+junction+` (material_id, `+refCol+`) VALUES ($1, $2)`, materialID, refID)
	return err
}

func (r *repository) insertMaterialJunctions(ctx context.Context, tx pgx.Tx, materialID uuid.UUID, c *Content) error {
	link := func(junction, refTable, refCol string, refID uuid.UUID) error {
		return r.linkMaterialJunction(ctx, tx, junction, refTable, refCol, materialID, refID)
	}
	if err := link("material_subject", "academic.subject", "subject_id", c.SubjectID); err != nil {
		return err
	}
	if err := link("material_grade", "academic.grade", "grade_id", c.GradeID); err != nil {
		return err
	}
	if c.ChapterID != nil {
		if err := link("material_chapter", "academic.chapter", "chapter_id", *c.ChapterID); err != nil {
			return err
		}
	}
	if c.TopicID != nil {
		if err := link("material_topic", "academic.topic", "topic_id", *c.TopicID); err != nil {
			return err
		}
	}
	return nil
}

func (r *repository) clearMaterialJunctions(ctx context.Context, tx pgx.Tx, materialID uuid.UUID) error {
	for _, t := range []string{"material_subject", "material_grade", "material_chapter", "material_topic"} {
		if _, err := tx.Exec(ctx, `DELETE FROM content.`+t+` WHERE material_id = $1`, materialID); err != nil {
			return err
		}
	}
	return nil
}

func materialSlug(title string) string {
	s := strings.ToLower(strings.TrimSpace(title))
	if s == "" {
		return "material"
	}
	re := regexp.MustCompile(`[^a-z0-9]+`)
	s = re.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	if s == "" {
		return "material"
	}
	return s
}

// createMaterialContent creates the content.material master plus its first
// version, body block and academic junctions in one transaction. The version
// + block + metadata/statistics split is: master+version+block+junctions here,
// metadata+statistics+history in CreateMaterial.
func (r *repository) createMaterialContent(ctx context.Context, c *Content) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	statuses, err := r.ensureMaterialStatuses(ctx, tx)
	if err != nil {
		return err
	}
	types, err := r.ensureMaterialTypes(ctx, tx)
	if err != nil {
		return err
	}

	code := "mat_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	slug := materialSlug(c.Title)
	if slug == "material" || slug == "" {
		slug = "material"
	}
	slug = slug + "-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8]

	var typeID *uuid.UUID
	if f, ok := c.Metadata["content_format"]; ok {
		var format string
		switch v := f.(type) {
		case MaterialFormat:
			format = string(v)
		case string:
			format = v
		}
		if t, ok := types[format]; ok {
			typeID = &t
		}
	}
	statusID := statuses[string(c.Status)]
	if statusID == uuid.Nil {
		statusID = statuses[string(StatusDraft)]
	}

	var publishedAt *time.Time
	if c.Status == StatusPublished {
		now := time.Now()
		publishedAt = &now
		c.PublishedAt = publishedAt
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO content.material (id, material_code, title, slug, summary, material_type_id, status_id, owner_id, created_by, updated_by, published_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		c.ID, code, c.Title, slug, c.Body, typeID, statusID, c.CreatedBy, c.CreatedBy, c.CreatedBy, publishedAt, c.CreatedAt, c.UpdatedAt)
	if err != nil {
		return err
	}

	var versionID uuid.UUID
	if err := tx.QueryRow(ctx, `
		INSERT INTO content.material_version (material_id, version_no, change_summary, created_by, is_current)
		VALUES ($1, 1, 'Initial version', $2, true) RETURNING id`, c.ID, c.CreatedBy).Scan(&versionID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO content.material_block (material_version_id, block_order, block_type, content)
		VALUES ($1, 0, 'PARAGRAPH', $2)`, versionID, c.Body); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE content.material SET current_version_id = $1 WHERE id = $2`, versionID, c.ID); err != nil {
		return err
	}

	if err := r.insertMaterialJunctions(ctx, tx, c.ID, c); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// updateMaterialContent updates master-level fields and junctions for a
// content.material row. The version bump lives in UpdateMaterial.
func (r *repository) updateMaterialContent(ctx context.Context, id uuid.UUID, req UpdateContentReq) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	statuses, err := r.ensureMaterialStatuses(ctx, tx)
	if err != nil {
		return err
	}

	var statusID *uuid.UUID
	if req.Status != nil {
		sid := statuses[string(*req.Status)]
		if sid != uuid.Nil {
			statusID = &sid
		}
	}

	// published_at is only written on an explicit status transition: set on
	// PUBLISHED, cleared when leaving PUBLISHED, untouched on nil-status edits.
	sets := []string{"title = COALESCE($2, title)", "summary = COALESCE($3, summary)", "status_id = COALESCE($4, status_id)"}
	args := []interface{}{id, req.Title, req.Body, statusID}
	argN := 5
	if req.Status != nil {
		sets = append(sets, fmt.Sprintf("published_at = $%d", argN))
		if *req.Status == StatusPublished {
			if req.PublishedAt != nil {
				args = append(args, *req.PublishedAt)
			} else {
				args = append(args, time.Now())
			}
		} else {
			args = append(args, nil)
		}
		argN++
	}
	sets = append(sets, "updated_at = NOW()")

	if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE content.material SET %s WHERE id = $1", strings.Join(sets, ", ")), args...); err != nil {
		return err
	}

	// Rebuild junctions on an academic-field edit; preserve the existing grade
	// link (UpdateContentReq carries no GradeID, so read it before clearing).
	if req.SubjectID != nil || req.ChapterID != nil || req.TopicID != nil {
		var curGrade uuid.UUID
		_ = tx.QueryRow(ctx, `
			SELECT COALESCE(gr.grade_id, '00000000-0000-0000-0000-000000000000')
			FROM (SELECT grade_id FROM content.material_grade WHERE material_id = $1 LIMIT 1) gr`, id).Scan(&curGrade)

		if err := r.clearMaterialJunctions(ctx, tx, id); err != nil {
			return err
		}
		if err := r.insertMaterialJunctions(ctx, tx, id, &Content{
			SubjectID: ptrUUIDOrNil(req.SubjectID),
			GradeID:   curGrade,
			ChapterID: req.ChapterID,
			TopicID:   req.TopicID,
		}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func ptrUUIDOrNil(u *uuid.UUID) uuid.UUID {
	if u == nil {
		return uuid.Nil
	}
	return *u
}

func (r *repository) GetContent(ctx context.Context, id uuid.UUID) (*Content, error) {
	// EXAM masters live in cbt.exam; project them back into the Content shape.
	var isExam bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM cbt.exam WHERE id = $1 AND deleted_at IS NULL)`, id).Scan(&isExam); err == nil && isExam {
		c := &Content{}
		var meta map[string]interface{}
		err := r.pool.QueryRow(ctx, `
			SELECT m.id, 'EXAM',
			       COALESCE(gr.grade_id, '00000000-0000-0000-0000-000000000000')::uuid,
			       COALESCE(subj.subject_id, '00000000-0000-0000-0000-000000000000')::uuid,
			       ch.chapter_id, tp.topic_id, NULL::uuid, m.title,
			       COALESCE(m.description, '')::text, COALESCE(st.code, 'DRAFT')::text, m.owner_id,
			       NULL::jsonb, NULL::timestamptz, m.created_at, m.updated_at
			FROM cbt.exam m
			LEFT JOIN cbt.exam_status st ON st.id = m.status_id
			LEFT JOIN LATERAL (SELECT grade_id FROM cbt.exam_grade WHERE exam_id = m.id LIMIT 1) gr ON true
			LEFT JOIN LATERAL (SELECT subject_id FROM cbt.exam_subject WHERE exam_id = m.id LIMIT 1) subj ON true
			LEFT JOIN LATERAL (SELECT chapter_id FROM cbt.exam_chapter WHERE exam_id = m.id LIMIT 1) ch ON true
			LEFT JOIN LATERAL (SELECT topic_id FROM cbt.exam_topic WHERE exam_id = m.id LIMIT 1) tp ON true
			WHERE m.id = $1`, id).Scan(
			&c.ID, &c.ContentType, &c.GradeID, &c.SubjectID, &c.ChapterID, &c.TopicID, &c.LOID,
			&c.Title, &c.Body, &c.Status, &c.CreatedBy, &meta, &c.PublishedAt, &c.CreatedAt, &c.UpdatedAt)
		if err != nil {
			return nil, err
		}
		if len(meta) > 0 {
			c.Metadata = meta
		}
		return c, nil
	}

	// MATERIAL masters live in content.material.
	var isMaterial bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM content.material WHERE id = $1 AND deleted_at IS NULL)`, id).Scan(&isMaterial); err == nil && isMaterial {
		c := &Content{}
		var meta map[string]interface{}
		err := r.pool.QueryRow(ctx, `
			SELECT m.id, 'MATERIAL',
			       COALESCE(gr.grade_id, '00000000-0000-0000-0000-000000000000')::uuid,
			       COALESCE(subj.subject_id, '00000000-0000-0000-0000-000000000000')::uuid,
			       ch.chapter_id, tp.topic_id, NULL::uuid, m.title,
			       COALESCE(blk.content, COALESCE(m.summary, ''))::text, COALESCE(st.code, 'DRAFT')::text, COALESCE(m.owner_id, '00000000-0000-0000-0000-000000000000')::uuid,
			       NULL::jsonb, m.published_at, m.created_at, m.updated_at
			FROM content.material m
			LEFT JOIN content.material_status st ON st.id = m.status_id
			LEFT JOIN content.material_version mv ON mv.id = m.current_version_id
			LEFT JOIN content.material_block blk ON blk.material_version_id = mv.id AND blk.block_order = 0
			LEFT JOIN LATERAL (SELECT subject_id FROM content.material_subject WHERE material_id = m.id LIMIT 1) subj ON true
			LEFT JOIN LATERAL (SELECT grade_id FROM content.material_grade WHERE material_id = m.id LIMIT 1) gr ON true
			LEFT JOIN LATERAL (SELECT chapter_id FROM content.material_chapter WHERE material_id = m.id LIMIT 1) ch ON true
			LEFT JOIN LATERAL (SELECT topic_id FROM content.material_topic WHERE material_id = m.id LIMIT 1) tp ON true
			WHERE m.id = $1`, id).Scan(
			&c.ID, &c.ContentType, &c.GradeID, &c.SubjectID, &c.ChapterID, &c.TopicID, &c.LOID,
			&c.Title, &c.Body, &c.Status, &c.CreatedBy, &meta, &c.PublishedAt, &c.CreatedAt, &c.UpdatedAt)
		if err != nil {
			return nil, err
		}
		if len(meta) > 0 {
			c.Metadata = meta
		}
		return c, nil
	}

	// Remaining (QUESTION and other) content masters live in question.question.
	c := &Content{}
	var meta map[string]interface{}
	err := r.pool.QueryRow(ctx, `
		SELECT q.id, 'QUESTION',
		       COALESCE(gr.grade_id, '00000000-0000-0000-0000-000000000000')::uuid,
		       COALESCE(subj.subject_id, '00000000-0000-0000-0000-000000000000')::uuid,
		       ch.chapter_id, tp.topic_id, NULL::uuid, q.question_code,
		       COALESCE(blk.content, '')::text, COALESCE(st.code, 'DRAFT')::text, COALESCE(q.owner_id, '00000000-0000-0000-0000-000000000000')::uuid,
		       NULL::jsonb, NULL::timestamptz, q.created_at, q.updated_at
		FROM question.question q
		LEFT JOIN question.question_status st ON st.id = q.status_id
		LEFT JOIN question.question_version v ON v.id = q.current_version_id
		LEFT JOIN question.question_block blk ON blk.question_version_id = v.id AND blk.block_order = 0
		LEFT JOIN LATERAL (SELECT subject_id FROM question.question_subject WHERE question_id = q.id LIMIT 1) subj ON true
		LEFT JOIN LATERAL (SELECT grade_id FROM question.question_grade WHERE question_id = q.id LIMIT 1) gr ON true
		LEFT JOIN LATERAL (SELECT chapter_id FROM question.question_chapter WHERE question_id = q.id LIMIT 1) ch ON true
		LEFT JOIN LATERAL (SELECT topic_id FROM question.question_topic WHERE question_id = q.id LIMIT 1) tp ON true
		WHERE q.id = $1 AND q.deleted_at IS NULL`, id).Scan(
		&c.ID, &c.ContentType, &c.GradeID, &c.SubjectID, &c.ChapterID, &c.TopicID, &c.LOID,
		&c.Title, &c.Body, &c.Status, &c.CreatedBy, &meta, &c.PublishedAt, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		return nil, err
	}
	if len(meta) > 0 {
		c.Metadata = meta
	}
	return c, nil
}

func (r *repository) UpdateContent(ctx context.Context, id uuid.UUID, req UpdateContentReq) error {
	sets := []string{}
	args := []interface{}{}
	argN := 1

	if req.Title != nil {
		sets = append(sets, fmt.Sprintf("title = $%d", argN))
		args = append(args, *req.Title)
		argN++
	}
	if req.Body != nil {
		sets = append(sets, fmt.Sprintf("body = $%d", argN))
		args = append(args, *req.Body)
		argN++
	}
	if req.Status != nil {
		sets = append(sets, fmt.Sprintf("status = $%d", argN))
		args = append(args, *req.Status)
		argN++
		if *req.Status == StatusPublished && req.PublishedAt == nil {
			now := time.Now()
			req.PublishedAt = &now
		}
	}
	if req.SubjectID != nil {
		sets = append(sets, fmt.Sprintf("subject_id = $%d", argN))
		args = append(args, *req.SubjectID)
		argN++
	}
	if req.ChapterID != nil {
		sets = append(sets, fmt.Sprintf("chapter_id = $%d", argN))
		args = append(args, *req.ChapterID)
		argN++
	}
	if req.TopicID != nil {
		sets = append(sets, fmt.Sprintf("topic_id = $%d", argN))
		args = append(args, *req.TopicID)
		argN++
	}
	if req.LOID != nil {
		sets = append(sets, fmt.Sprintf("lo_id = $%d", argN))
		args = append(args, *req.LOID)
		argN++
	}
	if req.Metadata != nil {
		sets = append(sets, fmt.Sprintf("metadata = $%d", argN))
		args = append(args, req.Metadata)
		argN++
	}
	if req.PublishedAt != nil {
		sets = append(sets, fmt.Sprintf("published_at = $%d", argN))
		args = append(args, *req.PublishedAt)
		argN++
	}

	if len(sets) == 0 {
		return nil
	}

	// Route updates by master type. The legacy `contents` UPDATE no longer
	// exists; question/exam/material masters all live in the new schema.
	var isMaterial bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM content.material WHERE id = $1 AND deleted_at IS NULL)`, id).Scan(&isMaterial); err == nil && isMaterial {
		return r.updateMaterialContent(ctx, id, req)
	}

	// EXAM masters update against cbt.exam.
	var isExam bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM cbt.exam WHERE id = $1 AND deleted_at IS NULL)`, id).Scan(&isExam); err == nil && isExam {
		return r.updateExamContent(ctx, id, req)
	}

	// QUESTION masters update against question.question.
	var isQuestion bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM question.question WHERE id = $1 AND deleted_at IS NULL)`, id).Scan(&isQuestion); err == nil && isQuestion {
		return r.updateQuestionContent(ctx, id, req)
	}

	return pgx.ErrNoRows
}

// updateQuestionContent updates question.question field-level columns (such as
// status) and rebuilds the academic junctions on an academic-field edit. The
// body block lives on the current version's question_block, so a body edit made
// through the generic UpdateContent path is applied there without bumping a
// version (the typed UpdateQuestion bumps versions).
func (r *repository) updateQuestionContent(ctx context.Context, id uuid.UUID, req UpdateContentReq) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	statuses, err := r.ensureQuestionStatuses(ctx, tx)
	if err != nil {
		return err
	}
	var statusID *uuid.UUID
	if req.Status != nil {
		sid := statuses[string(*req.Status)]
		if sid != uuid.Nil {
			statusID = &sid
		}
	}

	sets := []string{"updated_at = NOW()"}
	args := []interface{}{id}
	if statusID != nil {
		sets = append(sets, fmt.Sprintf("status_id = $%d", len(args)))
		args = append(args, *statusID)
	}
	if len(sets) > 1 {
		if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE question.question SET %s WHERE id = $1 AND deleted_at IS NULL", strings.Join(sets, ", ")), args...); err != nil {
			return err
		}
	}

	if req.Body != nil {
		// Apply a body edit to the current version's PARAGRAPH block so a
		// generic UpdateContent(body) round-trips without a version bump.
		if _, err := tx.Exec(ctx, `
			UPDATE question.question_block blk SET content = $2
			FROM question.question q
			WHERE q.id = $1 AND q.deleted_at IS NULL
			  AND q.current_version_id = blk.question_version_id
			  AND blk.block_order = 0`, id, *req.Body); err != nil {
			return err
		}
	}

	if req.SubjectID != nil || req.ChapterID != nil || req.TopicID != nil {
		var curGrade uuid.UUID
		_ = tx.QueryRow(ctx, `
			SELECT COALESCE(gr.grade_id, '00000000-0000-0000-0000-000000000000')
			FROM (SELECT grade_id FROM question.question_grade WHERE question_id = $1 LIMIT 1) gr`, id).Scan(&curGrade)
		if err := r.clearQuestionJunctions(ctx, tx, id); err != nil {
			return err
		}
		if err := r.insertQuestionJunctions(ctx, tx, id, &Content{
			SubjectID: ptrUUIDOrNil(req.SubjectID),
			GradeID:   curGrade,
			ChapterID: req.ChapterID,
			TopicID:   req.TopicID,
		}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (r *repository) DeleteContent(ctx context.Context, id uuid.UUID) error {
	// Route deletes by master type.
	var isMaterial bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM content.material WHERE id = $1)`, id).Scan(&isMaterial); err == nil && isMaterial {
		return r.DeleteMaterial(ctx, id)
	}

	// EXAM masters soft-delete against cbt.exam.
	var isExam bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM cbt.exam WHERE id = $1)`, id).Scan(&isExam); err == nil && isExam {
		return r.softDeleteExam(ctx, id)
	}

	// QUESTION masters soft-delete against question.question.
	var isQuestion bool
	if err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM question.question WHERE id = $1)`, id).Scan(&isQuestion); err == nil && isQuestion {
		return r.DeleteQuestion(ctx, id)
	}
	return pgx.ErrNoRows
}

func (r *repository) GetUserGradeID(ctx context.Context, userID uuid.UUID) (*uuid.UUID, error) {
	var gradeID *uuid.UUID
	err := r.pool.QueryRow(ctx, `
		SELECT grade_id FROM academic.student_enrollment
		WHERE student_id = $1 AND status = 'ACTIVE'
		ORDER BY updated_at DESC LIMIT 1`, userID).Scan(&gradeID)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return gradeID, nil
}

// IsContentAccessible checks if a student can access the content based on their grade enrollment.
// Returns true if the content's grade matches the student's enrolled grade, or if content has no grade restriction.
func (r *repository) IsContentAccessible(ctx context.Context, contentID, userID uuid.UUID) (bool, error) {
	// Get the content's grade ID
	var contentGradeID *uuid.UUID
	err := r.pool.QueryRow(ctx, `
		SELECT grade_id FROM (
			SELECT grade_id FROM cbt.exam WHERE id = $1 AND deleted_at IS NULL
			UNION ALL
			SELECT grade_id FROM content.material WHERE id = $1 AND deleted_at IS NULL
			UNION ALL
			SELECT grade_id FROM content.question WHERE id = $1 AND deleted_at IS NULL
		) t WHERE grade_id IS NOT NULL LIMIT 1
	`, contentID).Scan(&contentGradeID)
	if err != nil {
		if err == pgx.ErrNoRows {
			// Content has no grade restriction, accessible to all
			return true, nil
		}
		return false, err
	}

	if contentGradeID == nil {
		// Content has no grade restriction
		return true, nil
	}

	// Get student's enrolled grade
	studentGradeID, err := r.GetUserGradeID(ctx, userID)
	if err != nil {
		return false, err
	}
	if studentGradeID == nil {
		// Student not enrolled in any grade
		return false, nil
	}

	// Check if grades match
	return *contentGradeID == *studentGradeID, nil
}

// listContentBranch returns a CTE subquery name + the unified projection for a
// given master type so ListContent can filter across material/exam/question
// masters with one ORDER BY/LIMIT. Each branch yields the 15-column Content shape
// with stable aliases (id, content_type, grade_id, subject_id, chapter_id,
// topic_id, lo_id, title, body, status, created_by, metadata, published_at,
// created_at, updated_at).
func listContentBranch(kind string) string {
	switch kind {
	case "MATERIAL":
		return `
			SELECT m.id AS id, 'MATERIAL'::text AS content_type,
			       COALESCE(js.subject_id, '00000000-0000-0000-0000-000000000000')::uuid AS subject_id,
			       COALESCE(jg.grade_id, '00000000-0000-0000-0000-000000000000')::uuid AS grade_id,
			       jc.chapter_id AS chapter_id, jt.topic_id AS topic_id,
			       NULL::uuid AS lo_id,
			       m.title AS title,
			       COALESCE(mblk.content, COALESCE(m.summary, ''))::text AS body,
			       COALESCE(mst.code, 'DRAFT')::text AS status,
			       COALESCE(m.owner_id, '00000000-0000-0000-0000-000000000000')::uuid AS created_by,
			       NULL::jsonb AS metadata, m.published_at AS published_at, m.created_at AS created_at, m.updated_at AS updated_at
			FROM content.material m
			LEFT JOIN content.material_status mst ON mst.id = m.status_id
			LEFT JOIN content.material_version mv ON mv.id = m.current_version_id
			LEFT JOIN content.material_block mblk ON mblk.material_version_id = mv.id AND mblk.block_order = 0
			LEFT JOIN LATERAL (SELECT subject_id FROM content.material_subject WHERE material_id = m.id LIMIT 1) js ON true
			LEFT JOIN LATERAL (SELECT grade_id FROM content.material_grade WHERE material_id = m.id LIMIT 1) jg ON true
			LEFT JOIN LATERAL (SELECT chapter_id FROM content.material_chapter WHERE material_id = m.id LIMIT 1) jc ON true
			LEFT JOIN LATERAL (SELECT topic_id FROM content.material_topic WHERE material_id = m.id LIMIT 1) jt ON true
			WHERE m.deleted_at IS NULL`
	case "EXAM":
		return `
			SELECT m.id AS id, 'EXAM'::text AS content_type,
			       COALESCE(js.subject_id, '00000000-0000-0000-0000-000000000000')::uuid AS subject_id,
			       COALESCE(jg.grade_id, '00000000-0000-0000-0000-000000000000')::uuid AS grade_id,
			       jc.chapter_id AS chapter_id, jt.topic_id AS topic_id,
			       NULL::uuid AS lo_id,
			       m.title AS title, COALESCE(m.description, '')::text AS body,
			       COALESCE(est.code, 'DRAFT')::text AS status,
			       COALESCE(m.owner_id, '00000000-0000-0000-0000-000000000000')::uuid AS created_by,
			       NULL::jsonb AS metadata, NULL::timestamptz AS published_at, m.created_at AS created_at, m.updated_at AS updated_at
			FROM cbt.exam m
			LEFT JOIN cbt.exam_status est ON est.id = m.status_id
			LEFT JOIN LATERAL (SELECT subject_id FROM cbt.exam_subject WHERE exam_id = m.id LIMIT 1) js ON true
			LEFT JOIN LATERAL (SELECT grade_id FROM cbt.exam_grade WHERE exam_id = m.id LIMIT 1) jg ON true
			LEFT JOIN LATERAL (SELECT chapter_id FROM cbt.exam_chapter WHERE exam_id = m.id LIMIT 1) jc ON true
			LEFT JOIN LATERAL (SELECT topic_id FROM cbt.exam_topic WHERE exam_id = m.id LIMIT 1) jt ON true
			WHERE m.deleted_at IS NULL`
	default: // QUESTION
		return `
			SELECT q.id AS id, 'QUESTION'::text AS content_type,
			       COALESCE(qs.subject_id, '00000000-0000-0000-0000-000000000000')::uuid AS subject_id,
			       COALESCE(qg.grade_id, '00000000-0000-0000-0000-000000000000')::uuid AS grade_id,
			       qc.chapter_id AS chapter_id, qt.topic_id AS topic_id,
			       NULL::uuid AS lo_id,
			       q.question_code AS title, COALESCE(qblk.content, '')::text AS body,
			       COALESCE(qst.code, 'DRAFT')::text AS status,
			       COALESCE(q.owner_id, '00000000-0000-0000-0000-000000000000')::uuid AS created_by,
			       NULL::jsonb AS metadata, NULL::timestamptz AS published_at, q.created_at AS created_at, q.updated_at AS updated_at
			FROM question.question q
			LEFT JOIN question.question_status qst ON qst.id = q.status_id
			LEFT JOIN question.question_version qv ON qv.id = q.current_version_id
			LEFT JOIN question.question_block qblk ON qblk.question_version_id = qv.id AND qblk.block_order = 0
			LEFT JOIN LATERAL (SELECT subject_id FROM question.question_subject WHERE question_id = q.id LIMIT 1) qs ON true
			LEFT JOIN LATERAL (SELECT grade_id FROM question.question_grade WHERE question_id = q.id LIMIT 1) qg ON true
			LEFT JOIN LATERAL (SELECT chapter_id FROM question.question_chapter WHERE question_id = q.id LIMIT 1) qc ON true
			LEFT JOIN LATERAL (SELECT topic_id FROM question.question_topic WHERE question_id = q.id LIMIT 1) qt ON true
			WHERE q.deleted_at IS NULL`
	}
}

func (r *repository) ListContent(ctx context.Context, filter ContentFilter) ([]Content, int, error) {
	branches := []string{
		listContentBranch("MATERIAL"),
		listContentBranch("EXAM"),
		listContentBranch("QUESTION"),
	}
	if filter.ContentType != nil {
		switch *filter.ContentType {
		case ContentTypeMaterial:
			branches = branches[:1]
		case ContentTypeExam:
			branches = branches[1:2]
		case ContentTypeQuestion:
			branches = branches[2:]
		}
	}
	from := "FROM ((" + strings.Join(branches, ") UNION ALL (") + ")) u"

	where := " WHERE 1=1"
	args := []interface{}{}
	argN := 1
	if filter.GradeID != nil {
		where += fmt.Sprintf(" AND u.grade_id = $%d", argN)
		args = append(args, *filter.GradeID)
		argN++
	}
	if filter.SubjectID != nil {
		where += fmt.Sprintf(" AND u.subject_id = $%d", argN)
		args = append(args, *filter.SubjectID)
		argN++
	}
	if filter.Status != nil {
		where += fmt.Sprintf(" AND u.status = $%d", argN)
		args = append(args, *filter.Status)
		argN++
	}
	if filter.CreatedBy != nil {
		where += fmt.Sprintf(" AND u.created_by = $%d", argN)
		args = append(args, *filter.CreatedBy)
		argN++
	}
	if filter.Search != "" {
		where += fmt.Sprintf(" AND (u.title ILIKE $%d OR u.body ILIKE $%d)", argN, argN)
		args = append(args, "%"+filter.Search+"%")
		argN++
	}

	var total int
	if err := r.pool.QueryRow(ctx, "SELECT COUNT(*) "+from+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 20
	}
	offset := filter.Offset

	query := fmt.Sprintf(`
		SELECT u.id, u.content_type, u.grade_id, u.subject_id, u.chapter_id, u.topic_id, u.lo_id, u.title, u.body, u.status, u.created_by, u.metadata, u.published_at, u.created_at, u.updated_at
		%s%s ORDER BY u.created_at DESC LIMIT $%d OFFSET $%d`, from, where, argN, argN+1)
	args = append(args, limit, offset)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var contents []Content
	for rows.Next() {
		var c Content
		if err := rows.Scan(&c.ID, &c.ContentType, &c.GradeID, &c.SubjectID, &c.ChapterID, &c.TopicID, &c.LOID, &c.Title, &c.Body, &c.Status, &c.CreatedBy, &c.Metadata, &c.PublishedAt, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, 0, err
		}
		contents = append(contents, c)
	}
	return contents, total, rows.Err()
}

func (r *repository) GetContentByGrade(ctx context.Context, gradeID uuid.UUID, contentType ContentType, limit, offset int) ([]Content, int, error) {
	filter := ContentFilter{
		GradeID:     &gradeID,
		ContentType: &contentType,
		Limit:       limit,
		Offset:      offset,
	}
	return r.ListContent(ctx, filter)
}

// ========== QUESTION SCHEMA HELPERS (question.*) ==========

// ensureQuestionStatuses idempotently seeds the question_status lookup rows and
// returns a code->id map. Safe to call on every write.
func (r *repository) ensureQuestionStatuses(ctx context.Context, tx pgx.Tx) (map[string]uuid.UUID, error) {
	rows := [][2]string{
		{"DRAFT", "Draft"},
		{"REVIEW", "In Review"},
		{"APPROVED", "Approved"},
		{"PUBLISHED", "Published"},
		{"ARCHIVED", "Archived"},
	}
	for _, s := range rows {
		if _, err := tx.Exec(ctx, `INSERT INTO question.question_status (code, name) VALUES ($1, $2) ON CONFLICT (code) DO NOTHING`, s[0], s[1]); err != nil {
			return nil, err
		}
	}
	statuses := map[string]uuid.UUID{}
	rws, err := tx.Query(ctx, `SELECT code, id FROM question.question_status WHERE code = ANY($1)`, []string{"DRAFT", "REVIEW", "APPROVED", "PUBLISHED", "ARCHIVED"})
	if err != nil {
		return nil, err
	}
	defer rws.Close()
	for rws.Next() {
		var code string
		var id uuid.UUID
		if err := rws.Scan(&code, &id); err != nil {
			return nil, err
		}
		statuses[code] = id
	}
	return statuses, rws.Err()
}

// linkQuestionJunction inserts an N:M row only when the referenced academic row
// exists, so an empty academic catalog degrades gracefully.
func (r *repository) linkQuestionJunction(ctx context.Context, tx pgx.Tx, junction, refTable, refCol string, questionID, refID uuid.UUID) error {
	if refID == uuid.Nil {
		return nil
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+refTable+` WHERE id = $1)`, refID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	_, err := tx.Exec(ctx, `INSERT INTO question.`+junction+` (question_id, `+refCol+`) VALUES ($1, $2)`, questionID, refID)
	return err
}

func (r *repository) insertQuestionJunctions(ctx context.Context, tx pgx.Tx, questionID uuid.UUID, c *Content) error {
	link := func(junction, refTable, refCol string, refID uuid.UUID) error {
		return r.linkQuestionJunction(ctx, tx, junction, refTable, refCol, questionID, refID)
	}
	if err := link("question_subject", "academic.subject", "subject_id", c.SubjectID); err != nil {
		return err
	}
	if err := link("question_grade", "academic.grade", "grade_id", c.GradeID); err != nil {
		return err
	}
	if c.ChapterID != nil {
		if err := link("question_chapter", "academic.chapter", "chapter_id", *c.ChapterID); err != nil {
			return err
		}
	}
	if c.TopicID != nil {
		if err := link("question_topic", "academic.topic", "topic_id", *c.TopicID); err != nil {
			return err
		}
	}
	return nil
}

func (r *repository) clearQuestionJunctions(ctx context.Context, tx pgx.Tx, questionID uuid.UUID) error {
	for _, t := range []string{"question_subject", "question_grade", "question_chapter", "question_topic"} {
		if _, err := tx.Exec(ctx, `DELETE FROM question.`+t+` WHERE question_id = $1`, questionID); err != nil {
			return err
		}
	}
	return nil
}

// createQuestionContent creates the question.question master plus its first
// version, body block, metadata and academic junctions in one transaction.
// Question content rows have no contents-row counterpart; the question master
// IS the content row (ContentType QUESTION dispatches here from CreateContent).
func (r *repository) createQuestionContent(ctx context.Context, c *Content) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	statuses, err := r.ensureQuestionStatuses(ctx, tx)
	if err != nil {
		return err
	}
	statusID := statuses[string(c.Status)]
	if statusID == uuid.Nil {
		statusID = statuses[string(StatusDraft)]
	}

	qType := QuestionTypeSingleChoice
	if t, ok := c.Metadata["question_type"].(string); ok && t != "" {
		qType = QuestionType(t)
	}

	code := "Q" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if _, err := tx.Exec(ctx, `
		INSERT INTO question.question (id, question_code, question_type, status_id, owner_id, created_by, updated_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		c.ID, code, qType, statusID, c.CreatedBy, c.CreatedBy, c.CreatedBy); err != nil {
		return err
	}

	var versionID uuid.UUID
	if err := tx.QueryRow(ctx, `
		INSERT INTO question.question_version (question_id, version_no, change_summary, created_by, is_current)
		VALUES ($1, 1, 'Initial version', $2, true) RETURNING id`, c.ID, c.CreatedBy).Scan(&versionID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE question.question SET current_version_id = $1 WHERE id = $2`, versionID, c.ID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO question.question_block (question_version_id, block_order, block_type, content)
		VALUES ($1, 0, 'PARAGRAPH', $2)`, versionID, c.Body); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO question.question_metadata (question_id, language)
		VALUES ($1, 'id') ON CONFLICT (question_id) DO NOTHING`, c.ID); err != nil {
		return err
	}

	if err := r.insertQuestionJunctions(ctx, tx, c.ID, c); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// questionColumns projects a question.question master plus its current version,
// metadata, academic junctions and status into the QuestionFull shape. The
// body block content feeds both the Content.Title (question_code) and
// Content.Body fields (there is no separate contents row for questions).
const questionColumns = `
	q.id,
	'QUESTION'::text,
	COALESCE(gr.grade_id, '00000000-0000-0000-0000-000000000000')::uuid,
	COALESCE(subj.subject_id, '00000000-0000-0000-0000-000000000000')::uuid,
	ch.chapter_id,
	tp.topic_id,
	NULL::uuid,
	q.question_code,
	COALESCE(blk.content, '')::text,
	COALESCE(st.code, 'DRAFT')::text,
	COALESCE(q.owner_id, '00000000-0000-0000-0000-000000000000')::uuid,
	NULL::jsonb,
	NULL::timestamptz,
	q.created_at,
	q.updated_at,
	q.question_type::text,
	COALESCE(md.difficulty_level, 'MEDIUM')::text,
	md.blooms_level::text,
	md.cognitive_level::text,
	COALESCE(md.language, 'id')::text,
	COALESCE(md.source_name, 'MANUAL')::text,
	NULL::uuid,
	NULL::uuid,
	COALESCE((SELECT MAX(op.score) FROM question.question_option op WHERE op.question_version_id = v.id AND op.is_correct AND op.score > 0), 1.0)::float8,
	0.0::float8,
	COALESCE(md.estimated_time, 0),
	COALESCE(e.content, '')::text`

const questionFrom = `
	FROM question.question q
	LEFT JOIN question.question_status st ON st.id = q.status_id
	LEFT JOIN question.question_version v ON v.id = q.current_version_id
	LEFT JOIN question.question_block blk ON blk.question_version_id = v.id AND blk.block_order = 0
	LEFT JOIN question.question_metadata md ON md.question_id = q.id
	LEFT JOIN question.explanation e ON e.question_version_id = v.id
	LEFT JOIN LATERAL (SELECT subject_id FROM question.question_subject WHERE question_id = q.id LIMIT 1) subj ON true
	LEFT JOIN LATERAL (SELECT grade_id FROM question.question_grade WHERE question_id = q.id LIMIT 1) gr ON true
	LEFT JOIN LATERAL (SELECT chapter_id FROM question.question_chapter WHERE question_id = q.id LIMIT 1) ch ON true
	LEFT JOIN LATERAL (SELECT topic_id FROM question.question_topic WHERE question_id = q.id LIMIT 1) tp ON true`

func scanQuestionFull(row pgx.Row) (*QuestionFull, error) {
	q := &QuestionFull{}
	var meta map[string]interface{}
	var qType, difficulty, bloom, thinking, language, source string
	if err := row.Scan(
		&q.Content.ID, &q.Content.ContentType, &q.Content.GradeID, &q.Content.SubjectID, &q.Content.ChapterID, &q.Content.TopicID, &q.Content.LOID, &q.Content.Title, &q.Content.Body, &q.Content.Status, &q.Content.CreatedBy, &meta, &q.Content.PublishedAt, &q.Content.CreatedAt, &q.Content.UpdatedAt,
		&qType, &difficulty, &bloom, &thinking, &language, &source, &q.Question.SubTopicID, &q.Question.StimulusID, &q.Question.Score, &q.Question.NegativeScore, &q.Question.EstimatedTime, &q.Question.Explanation,
	); err != nil {
		return nil, err
	}
	q.Question.QuestionType = QuestionType(qType)
	q.Question.Difficulty = Difficulty(difficulty)
	if bloom != "" {
		b := BloomLevel(bloom)
		q.Question.BloomLevel = &b
	}
	if thinking != "" {
		t := ThinkingLevel(thinking)
		q.Question.ThinkingLevel = &t
	}
	q.Question.Language = language
	q.Question.Source = QuestionSource(source)
	if len(meta) > 0 {
		q.Content.Metadata = meta
	}
	return q, nil
}

// loadQuestionOptions loads the current-version options of a question.
func (r *repository) loadQuestionOptions(ctx context.Context, questionID uuid.UUID) ([]QuestionOption, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT op.id, q.id, op.label, COALESCE(ob.content, '')::text, op.is_correct, op.display_order
		FROM question.question_option op
		JOIN question.question q ON q.current_version_id = op.question_version_id
		LEFT JOIN question.option_block ob ON ob.option_id = op.id AND ob.block_order = 0
		WHERE q.id = $1 AND q.deleted_at IS NULL
		ORDER BY op.display_order`, questionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var opts []QuestionOption
	for rows.Next() {
		var o QuestionOption
		if err := rows.Scan(&o.ID, &o.ContentID, &o.Label, &o.OptionText, &o.IsCorrect, &o.DisplayOrder); err != nil {
			return nil, err
		}
		opts = append(opts, o)
	}
	return opts, rows.Err()
}

// insertQuestionOptions writes the given options into a version, applying the
// score to the correct option. Create/Update/ReplaceOptions all funnel through
// here so scoring stays consistent.
func insertQuestionOptions(ctx context.Context, tx pgx.Tx, versionID uuid.UUID, opts []QuestionOption, score float64) error {
	for i, opt := range opts {
		opt.DisplayOrder = i
		opt.ID = uuid.New()
		optScore := 0.0
		if opt.IsCorrect {
			optScore = score
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO question.question_option (id, question_version_id, label, score, is_correct, display_order)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			opt.ID, versionID, opt.Label, optScore, opt.IsCorrect, opt.DisplayOrder); err != nil {
			return err
		}
		if opt.OptionText != "" {
			if _, err := tx.Exec(ctx, `
				INSERT INTO question.option_block (option_id, block_order, block_type, content)
				VALUES ($1, 0, 'PARAGRAPH', $2)`, opt.ID, opt.OptionText); err != nil {
				return err
			}
		}
	}
	return nil
}

// upsertQuestionMetadata mirrors question_bank.insertMetadata: difficulty and
// bloom values are normalized to the question_metadata CHECK domains.
func (r *repository) upsertQuestionMetadata(ctx context.Context, tx pgx.Tx, q *Question) error {
	var diff any
	if q.Difficulty != "" {
		diff = normalizeQuestionDifficulty(string(q.Difficulty))
	}
	var blooms any
	if q.BloomLevel != nil {
		if b := normalizeQuestionBloom(string(*q.BloomLevel)); b != "" {
			blooms = b
		}
	}
	var lang any
	if q.Language != "" {
		lang = q.Language
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO question.question_metadata
			(question_id, estimated_time, difficulty_level, blooms_level, cognitive_level, language, source_name)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (question_id) DO UPDATE SET
			estimated_time = COALESCE(EXCLUDED.estimated_time, question_metadata.estimated_time),
			difficulty_level = COALESCE(EXCLUDED.difficulty_level, question_metadata.difficulty_level),
			blooms_level = COALESCE(EXCLUDED.blooms_level, question_metadata.blooms_level),
			cognitive_level = COALESCE(EXCLUDED.cognitive_level, question_metadata.cognitive_level),
			language = COALESCE(EXCLUDED.language, question_metadata.language),
			source_name = COALESCE(EXCLUDED.source_name, question_metadata.source_name)`,
		q.ContentID, q.EstimatedTime, diff, blooms, q.ThinkingLevel, lang, q.Source)
	return err
}

func normalizeQuestionDifficulty(d string) string {
	switch strings.ToUpper(d) {
	case "EASY", "MEDIUM", "HARD", "VERY_HARD":
		return strings.ToUpper(d)
	}
	return "MEDIUM"
}

func normalizeQuestionBloom(b string) string {
	switch strings.ToUpper(b) {
	case "REMEMBER", "UNDERSTAND", "APPLY", "ANALYZE", "EVALUATE", "CREATE":
		return strings.ToUpper(b)
	}
	return ""
}

func (r *repository) CreateQuestion(ctx context.Context, q *Question, opts []QuestionOption) error {
	if q.ContentID == uuid.Nil {
		q.ContentID = uuid.New()
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	statuses, err := r.ensureQuestionStatuses(ctx, tx)
	if err != nil {
		return err
	}

	qType := q.QuestionType
	if qType == "" {
		qType = QuestionTypeSingleChoice
	}
	score := q.Score
	if score <= 0 {
		score = 1.0
	}

	code := "Q" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if _, err := tx.Exec(ctx, `
		INSERT INTO question.question (id, question_code, question_type, status_id)
		VALUES ($1, $2, $3, $4)`,
		q.ContentID, code, qType, statuses[string(StatusDraft)]); err != nil {
		return err
	}

	var versionID uuid.UUID
	if err := tx.QueryRow(ctx, `
		INSERT INTO question.question_version (question_id, version_no, change_summary, created_by, is_current)
		VALUES ($1, 1, 'Initial version', NULL, true) RETURNING id`, q.ContentID).Scan(&versionID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE question.question SET current_version_id = $1 WHERE id = $2`, versionID, q.ContentID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO question.question_block (question_version_id, block_order, block_type, content)
		VALUES ($1, 0, 'PARAGRAPH', $2)`, versionID, q.Explanation); err != nil {
		return err
	}
	if q.Explanation != "" {
		if _, err := tx.Exec(ctx, `
			INSERT INTO question.explanation (question_version_id, content) VALUES ($1, $2)`, versionID, q.Explanation); err != nil {
			return err
		}
	}
	if err := insertQuestionOptions(ctx, tx, versionID, opts, score); err != nil {
		return err
	}
	if err := r.upsertQuestionMetadata(ctx, tx, q); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (r *repository) GetQuestion(ctx context.Context, contentID uuid.UUID) (*QuestionFull, error) {
	q, err := scanQuestionFull(r.pool.QueryRow(ctx, `SELECT `+questionColumns+questionFrom+` WHERE q.id = $1 AND q.deleted_at IS NULL`, contentID))
	if err != nil {
		return nil, err
	}
	opts, err := r.loadQuestionOptions(ctx, contentID)
	if err != nil {
		return nil, err
	}
	q.Options = opts
	return q, nil
}

func (r *repository) UpdateQuestion(ctx context.Context, contentID uuid.UUID, q *Question) error {
	q.ContentID = contentID
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var curVer *uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT current_version_id FROM question.question WHERE id = $1 AND deleted_at IS NULL`, contentID).Scan(&curVer); err != nil {
		return err
	}
	if curVer == nil {
		return pgx.ErrNoRows
	}

	var nextNo int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(version_no), 0) + 1 FROM question.question_version WHERE question_id = $1`, contentID).Scan(&nextNo); err != nil {
		return err
	}

	// Carry the question score through the version bump: the correct option's
	// stored score IS the question score. Explicit positive q.Score wins;
	// otherwise reuse the current correct-option score.
	score := q.Score
	if score <= 0 {
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE((SELECT MAX(op.score) FROM question.question_option op
				WHERE op.question_version_id = $1 AND op.is_correct AND op.score > 0), 1.0)::float8`, *curVer).Scan(&score); err != nil {
			return err
		}
	}

	// Carry the body block through the version bump: the block content is the
	// question body, so a re-save keeps the current body unless the caller set
	// a new one (CreateQuestion stored body in the version's block).
	var body string
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(blk.content, '')
		FROM question.question_version v
		LEFT JOIN question.question_block blk ON blk.question_version_id = v.id AND blk.block_order = 0
		WHERE v.id = $1`, *curVer).Scan(&body); err != nil {
		return err
	}

	var newVer uuid.UUID
	if err := tx.QueryRow(ctx, `
		INSERT INTO question.question_version (question_id, version_no, change_summary, created_by, is_current)
		VALUES ($1, $2, 'content updated', NULL, true) RETURNING id`, contentID, nextNo).Scan(&newVer); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE question.question_version SET is_current = false WHERE id = $1`, *curVer); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE question.question SET current_version_id = $1, updated_at = NOW() WHERE id = $2`, newVer, contentID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO question.question_block (question_version_id, block_order, block_type, content)
		VALUES ($1, 0, 'PARAGRAPH', $2)`, newVer, body); err != nil {
		return err
	}
	if q.Explanation != "" {
		if _, err := tx.Exec(ctx, `
			INSERT INTO question.explanation (question_version_id, content) VALUES ($1, $2)`, newVer, q.Explanation); err != nil {
			return err
		}
	}
	if err := r.upsertQuestionMetadata(ctx, tx, q); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *repository) DeleteQuestion(ctx context.Context, contentID uuid.UUID) error {
	tag, err := r.pool.Exec(ctx, `UPDATE question.question SET deleted_at = NOW() WHERE id = $1 AND deleted_at IS NULL`, contentID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *repository) ListQuestions(ctx context.Context, filter QuestionFilter) ([]QuestionFull, int, error) {
	where := " WHERE q.deleted_at IS NULL"
	args := []interface{}{}
	argN := 1

	if filter.GradeID != nil {
		where += fmt.Sprintf(" AND gr.grade_id = $%d", argN)
		args = append(args, *filter.GradeID)
		argN++
	}
	if filter.SubjectID != nil {
		where += fmt.Sprintf(" AND subj.subject_id = $%d", argN)
		args = append(args, *filter.SubjectID)
		argN++
	}
	if filter.Status != nil {
		where += fmt.Sprintf(" AND st.code = $%d", argN)
		args = append(args, string(*filter.Status))
		argN++
	}
	if filter.CreatedBy != nil {
		where += fmt.Sprintf(" AND q.owner_id = $%d", argN)
		args = append(args, *filter.CreatedBy)
		argN++
	}
	if filter.Search != "" {
		where += fmt.Sprintf(" AND (q.question_code ILIKE $%d OR COALESCE(blk.content, '') ILIKE $%d)", argN, argN)
		args = append(args, "%"+filter.Search+"%")
		argN++
	}
	if filter.Difficulty != nil {
		where += fmt.Sprintf(" AND md.difficulty_level = $%d", argN)
		args = append(args, normalizeQuestionDifficulty(string(*filter.Difficulty)))
		argN++
	}
	if filter.BloomLevel != nil {
		where += fmt.Sprintf(" AND md.blooms_level = $%d", argN)
		args = append(args, normalizeQuestionBloom(string(*filter.BloomLevel)))
		argN++
	}
	if filter.ThinkingLevel != nil {
		where += fmt.Sprintf(" AND md.cognitive_level = $%d", argN)
		args = append(args, string(*filter.ThinkingLevel))
		argN++
	}
	if filter.QuestionType != nil {
		where += fmt.Sprintf(" AND q.question_type = $%d", argN)
		args = append(args, string(*filter.QuestionType))
		argN++
	}
	if filter.TopicID != nil {
		where += fmt.Sprintf(" AND tp.topic_id = $%d", argN)
		args = append(args, *filter.TopicID)
		argN++
	}

	var total int
	if err := r.pool.QueryRow(ctx, "SELECT COUNT(*) "+questionFrom+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 20
	}
	offset := filter.Offset

	query := fmt.Sprintf("SELECT "+questionColumns+questionFrom+where+
		" ORDER BY q.created_at DESC LIMIT $%d OFFSET $%d", argN, argN+1)
	args = append(args, limit, offset)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var questions []QuestionFull
	for rows.Next() {
		q, err := scanQuestionFull(rows)
		if err != nil {
			return nil, 0, err
		}
		opts, err := r.loadQuestionOptions(ctx, q.Content.ID)
		if err != nil {
			return nil, 0, err
		}
		q.Options = opts
		questions = append(questions, *q)
	}
	return questions, total, rows.Err()
}

func (r *repository) ReplaceOptions(ctx context.Context, contentID uuid.UUID, opts []QuestionOption) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var versionID uuid.UUID
	if err := tx.QueryRow(ctx, `
		SELECT current_version_id FROM question.question WHERE id = $1 AND deleted_at IS NULL`, contentID).Scan(&versionID); err != nil {
		return err
	}

	// Preserve the question score: the correct option's stored score IS the
	// question score. Read it before wiping options so a replace never resets
	// the score to 1.0.
	var score float64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE((SELECT MAX(op.score) FROM question.question_option op
			WHERE op.question_version_id = $1 AND op.is_correct AND op.score > 0), 1.0)::float8`, versionID).Scan(&score); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM question.option_block WHERE option_id IN
		(SELECT id FROM question.question_option WHERE question_version_id = $1)`, versionID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM question.question_option WHERE question_version_id = $1`, versionID); err != nil {
		return err
	}

	if err := insertQuestionOptions(ctx, tx, versionID, opts, score); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ========== MATERIALS ==========

// materialColumns projects a content.material master plus its current version
// block, metadata, statistics and academic names into a MaterialFull row.
// Junction access is 1:1 via LATERAL so a material always yields exactly one
// row even with multiple academic links.
const materialColumns = `
	m.id,
	'MATERIAL'::text,
	COALESCE(gr.grade_id, '00000000-0000-0000-0000-000000000000')::uuid,
	COALESCE(subj.subject_id, '00000000-0000-0000-0000-000000000000')::uuid,
	ch.chapter_id,
	tp.topic_id,
	NULL::uuid,
	m.title,
	COALESCE(blk.content, COALESCE(m.summary, ''))::text,
	COALESCE(st.code, 'DRAFT')::text,
	COALESCE(m.owner_id, '00000000-0000-0000-0000-000000000000')::uuid,
	NULL::jsonb,
	m.published_at,
	m.created_at,
	m.updated_at,
	m.id,
	COALESCE(mt.code, 'TEXT')::text,
	md.estimated_minutes,
	COALESCE(ms.view_count, 0),
	COALESCE(md.is_premium, false),
	NULL::uuid[],
	COALESCE(s.name, '')::text,
	COALESCE(ach.title, '')::text`

const materialFrom = `
	FROM content.material m
	LEFT JOIN content.material_status st ON st.id = m.status_id
	LEFT JOIN content.material_type mt ON mt.id = m.material_type_id
	LEFT JOIN content.material_version mv ON mv.id = m.current_version_id
	LEFT JOIN content.material_block blk ON blk.material_version_id = mv.id AND blk.block_order = 0
	LEFT JOIN content.material_metadata md ON md.material_id = m.id
	LEFT JOIN content.material_statistics ms ON ms.material_id = m.id
	LEFT JOIN LATERAL (SELECT subject_id FROM content.material_subject WHERE material_id = m.id LIMIT 1) subj ON true
	LEFT JOIN LATERAL (SELECT grade_id FROM content.material_grade WHERE material_id = m.id LIMIT 1) gr ON true
	LEFT JOIN LATERAL (SELECT chapter_id FROM content.material_chapter WHERE material_id = m.id LIMIT 1) ch ON true
	LEFT JOIN LATERAL (SELECT topic_id FROM content.material_topic WHERE material_id = m.id LIMIT 1) tp ON true
	LEFT JOIN academic.subject s ON s.id = subj.subject_id
	LEFT JOIN academic.grade g ON g.id = gr.grade_id
	LEFT JOIN academic.chapter ach ON ach.id = ch.chapter_id
	LEFT JOIN academic.topic t ON t.id = tp.topic_id`

func scanMaterial(row pgx.Row) (*MaterialFull, error) {
	m := &MaterialFull{}
	var meta map[string]interface{}
	if err := row.Scan(
		&m.Content.ID, &m.Content.ContentType, &m.Content.GradeID, &m.Content.SubjectID, &m.Content.ChapterID, &m.Content.TopicID, &m.Content.LOID, &m.Content.Title, &m.Content.Body, &m.Content.Status, &m.Content.CreatedBy, &meta, &m.Content.PublishedAt, &m.Content.CreatedAt, &m.Content.UpdatedAt,
		&m.Material.ContentID, &m.Material.ContentFormat, &m.Material.EstimatedDuration, &m.Material.ReadCount, &m.Material.IsPreview, &m.Material.Prerequisites,
		&m.SubjectName, &m.ChapterName,
	); err != nil {
		return nil, err
	}
	if len(meta) > 0 {
		m.Content.Metadata = meta
	}
	return m, nil
}

// CreateMaterial writes the metadata/statistics/history rows for a material
// master whose version + body block were created by CreateContent.
func (r *repository) CreateMaterial(ctx context.Context, m *Material) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO content.material_metadata (material_id, estimated_minutes, difficulty_level, language, is_premium)
		VALUES ($1, $2, $3, 'id', $4)
		ON CONFLICT (material_id) DO UPDATE SET
			estimated_minutes = EXCLUDED.estimated_minutes,
			is_premium = EXCLUDED.is_premium`,
		m.ContentID, m.EstimatedDuration, nil, m.IsPreview)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO content.material_statistics (material_id, view_count)
		VALUES ($1, $2)
		ON CONFLICT (material_id) DO UPDATE SET view_count = EXCLUDED.view_count`,
		m.ContentID, m.ReadCount)
	if err != nil {
		return err
	}

	for _, prereq := range m.Prerequisites {
		if _, err := tx.Exec(ctx, `
			INSERT INTO content.material_tag (material_id, tag) VALUES ($1, $2)
			ON CONFLICT (material_id, tag) DO NOTHING`, m.ContentID, "prereq:"+prereq.String()); err != nil {
			return err
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO content.material_history (material_id, action, new_json)
		VALUES ($1, 'CREATE', $2::jsonb)`, m.ContentID, `{"action":"CREATE"}`)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *repository) GetMaterial(ctx context.Context, contentID uuid.UUID) (*MaterialFull, error) {
	return scanMaterial(r.pool.QueryRow(ctx, `
		SELECT `+materialColumns+materialFrom+` WHERE m.id = $1 AND m.deleted_at IS NULL`, contentID))
}

// UpdateMaterial creates a new version (body carried from the master summary,
// which UpdateContent refreshed), flips the old current flag, and refreshes
// metadata + statistics + history atomically.
func (r *repository) UpdateMaterial(ctx context.Context, contentID uuid.UUID, m *Material) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var curVer *uuid.UUID
	if err := tx.QueryRow(ctx, `
		SELECT current_version_id FROM content.material WHERE id = $1 AND deleted_at IS NULL`, contentID).Scan(&curVer); err != nil {
		return err
	}
	if curVer == nil {
		return pgx.ErrNoRows
	}

	var nextNo int
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(version_no), 0) + 1 FROM content.material_version WHERE material_id = $1`, contentID).Scan(&nextNo); err != nil {
		return err
	}

	// Body lives on master.summary after UpdateContent; carry it into the new
	// version's block so GetMaterial reflects the updated body.
	var body string
	var ownerID *uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT COALESCE(summary, ''), owner_id FROM content.material WHERE id = $1`, contentID).Scan(&body, &ownerID); err != nil {
		return err
	}

	var newVer uuid.UUID
	if err := tx.QueryRow(ctx, `
		INSERT INTO content.material_version (material_id, version_no, change_summary, created_by, is_current)
		VALUES ($1, $2, 'content updated', $3, true) RETURNING id`, contentID, nextNo, ownerID).Scan(&newVer); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE content.material_version SET is_current = false WHERE id = $1`, *curVer); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE content.material SET current_version_id = $1, updated_by = $2, updated_at = NOW() WHERE id = $3`,
		newVer, ownerID, contentID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO content.material_block (material_version_id, block_order, block_type, content)
		VALUES ($1, 0, 'PARAGRAPH', $2)`, newVer, body); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO content.material_metadata (material_id, estimated_minutes, is_premium)
		VALUES ($1, $2, $3)
		ON CONFLICT (material_id) DO UPDATE SET
			estimated_minutes = EXCLUDED.estimated_minutes,
			is_premium = EXCLUDED.is_premium`,
		contentID, m.EstimatedDuration, m.IsPreview); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO content.material_statistics (material_id, view_count) VALUES ($1, 0)
		ON CONFLICT (material_id) DO NOTHING`, contentID); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO content.material_history (material_id, action, new_json)
		VALUES ($1, 'UPDATE', $2::jsonb)`, contentID, `{"action":"UPDATE"}`)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *repository) DeleteMaterial(ctx context.Context, contentID uuid.UUID) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var tag pgconn.CommandTag
	tag, err = tx.Exec(ctx, `
		UPDATE content.material SET deleted_at = NOW() WHERE id = $1 AND deleted_at IS NULL`, contentID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO content.material_history (material_id, action, new_json)
		VALUES ($1, 'DELETE', $2::jsonb)`, contentID, `{"action":"DELETE"}`)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *repository) ListMaterials(ctx context.Context, filter MaterialFilter) ([]MaterialFull, int, error) {
	where := " WHERE m.deleted_at IS NULL"
	args := []interface{}{}
	argN := 1

	if filter.SubjectID != nil {
		where += fmt.Sprintf(" AND subj.subject_id = $%d", argN)
		args = append(args, *filter.SubjectID)
		argN++
	}
	if filter.GradeID != nil {
		where += fmt.Sprintf(" AND gr.grade_id = $%d", argN)
		args = append(args, *filter.GradeID)
		argN++
	}
	if filter.Status != nil {
		where += fmt.Sprintf(" AND st.code = $%d", argN)
		args = append(args, string(*filter.Status))
		argN++
	}
	if filter.CreatedBy != nil {
		where += fmt.Sprintf(" AND m.owner_id = $%d", argN)
		args = append(args, *filter.CreatedBy)
		argN++
	}
	if filter.Search != "" {
		where += fmt.Sprintf(" AND (m.title ILIKE $%d OR COALESCE(blk.content, '') ILIKE $%d)", argN, argN)
		args = append(args, "%"+filter.Search+"%")
		argN++
	}
	if filter.ContentFormat != nil {
		where += fmt.Sprintf(" AND mt.code = $%d", argN)
		args = append(args, string(*filter.ContentFormat))
		argN++
	}

	var total int
	if err := r.pool.QueryRow(ctx, "SELECT COUNT(*) "+materialFrom+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 20
	}
	offset := filter.Offset

	query := fmt.Sprintf("SELECT "+materialColumns+materialFrom+where+
		" ORDER BY m.created_at DESC LIMIT $%d OFFSET $%d", argN, argN+1)
	args = append(args, limit, offset)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	materials := make([]MaterialFull, 0)
	for rows.Next() {
		m, err := scanMaterial(rows)
		if err != nil {
			return nil, 0, err
		}
		materials = append(materials, *m)
	}
	return materials, total, rows.Err()
}

func (r *repository) IncrementReadCount(ctx context.Context, contentID uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO content.material_statistics (material_id, view_count) VALUES ($1, 1)
		ON CONFLICT (material_id) DO UPDATE SET view_count = content.material_statistics.view_count + 1`, contentID)
	return err
}

// ========== LEARNING PROGRESS ==========

// parseLastPosition converts the legacy *string API field into the int column.
func parseLastPosition(s *string) int {
	if s == nil {
		return 0
	}
	n, err := strconv.Atoi(*s)
	if err != nil {
		return 0
	}
	return n
}

func lastPositionPtr(n int) *string {
	s := fmt.Sprint(n)
	return &s
}

func (r *repository) UpsertProgress(ctx context.Context, lp *LearningProgress) error {
	lp.ID = uuid.New()
	_, err := r.pool.Exec(ctx, `
		INSERT INTO content.learning_progress (id, student_id, material_id, progress_percent, last_position, completed, completed_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6, CASE WHEN $6 THEN NOW() ELSE NULL END, NOW(), NOW())
		ON CONFLICT (student_id, material_id) DO UPDATE SET
			progress_percent = $4,
			last_position = $5,
			completed = $6,
			completed_at = CASE WHEN EXCLUDED.completed THEN NOW() ELSE content.learning_progress.completed_at END,
			updated_at = NOW()
	`, lp.ID, lp.UserID, lp.MaterialID, lp.Progress, parseLastPosition(lp.LastPosition), lp.Completed)
	return err
}

func scanProgress(row pgx.Row) (*LearningProgress, error) {
	lp := &LearningProgress{}
	var lastPos int
	if err := row.Scan(&lp.ID, &lp.UserID, &lp.MaterialID, &lp.Progress, &lastPos, &lp.Completed, &lp.CreatedAt, &lp.UpdatedAt); err != nil {
		return nil, err
	}
	lp.LastPosition = lastPositionPtr(lastPos)
	return lp, nil
}

func (r *repository) GetProgress(ctx context.Context, userID, materialID uuid.UUID) (*LearningProgress, error) {
	return scanProgress(r.pool.QueryRow(ctx, `
		SELECT id, student_id, material_id, progress_percent::float8, last_position, completed, created_at, updated_at
		FROM content.learning_progress
		WHERE student_id=$1 AND material_id=$2
	`, userID, materialID))
}

func (r *repository) ListProgressByUser(ctx context.Context, userID uuid.UUID, limit, offset int) ([]LearningProgress, int, error) {
	var total int
	if err := r.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM content.learning_progress lp
		JOIN content.material m ON m.id = lp.material_id AND m.deleted_at IS NULL
		WHERE lp.student_id=$1`, userID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.pool.Query(ctx, `
		SELECT lp.id, lp.student_id, lp.material_id, lp.progress_percent::float8, lp.last_position, lp.completed, lp.created_at, lp.updated_at
		FROM content.learning_progress lp
		JOIN content.material m ON m.id = lp.material_id AND m.deleted_at IS NULL
		WHERE lp.student_id=$1 ORDER BY lp.updated_at DESC LIMIT $2 OFFSET $3`, userID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	list := make([]LearningProgress, 0)
	for rows.Next() {
		lp, err := scanProgress(rows)
		if err != nil {
			return nil, 0, err
		}
		list = append(list, *lp)
	}
	return list, total, rows.Err()
}

// ========== EXAMS ==========

// ensureExamStatuses idempotently seeds the cbt.exam_status lookup rows and
// returns a code->id map. Safe to call on every write.
func (r *repository) ensureExamStatuses(ctx context.Context, tx pgx.Tx) (map[string]uuid.UUID, error) {
	rows := [][2]string{
		{"DRAFT", "Draft"},
		{"REVIEW", "In Review"},
		{"APPROVED", "Approved"},
		{"PUBLISHED", "Published"},
		{"ARCHIVED", "Archived"},
	}
	for _, s := range rows {
		if _, err := tx.Exec(ctx, `INSERT INTO cbt.exam_status (code, name) VALUES ($1, $2) ON CONFLICT (code) DO NOTHING`, s[0], s[1]); err != nil {
			return nil, err
		}
	}
	statuses := map[string]uuid.UUID{}
	rws, err := tx.Query(ctx, `SELECT code, id FROM cbt.exam_status WHERE code = ANY($1)`,
		[]string{"DRAFT", "REVIEW", "APPROVED", "PUBLISHED", "ARCHIVED"})
	if err != nil {
		return nil, err
	}
	defer rws.Close()
	for rws.Next() {
		var code string
		var id uuid.UUID
		if err := rws.Scan(&code, &id); err != nil {
			return nil, err
		}
		statuses[code] = id
	}
	return statuses, rws.Err()
}

// linkExamJunction inserts an N:M row only when the referenced academic row
// exists, so an empty academic catalog degrades gracefully.
func (r *repository) linkExamJunction(ctx context.Context, tx pgx.Tx, junction, refTable, refCol string, examID, refID uuid.UUID) error {
	if refID == uuid.Nil {
		return nil
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+refTable+` WHERE id = $1)`, refID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	_, err := tx.Exec(ctx, `INSERT INTO cbt.`+junction+` (exam_id, `+refCol+`) VALUES ($1, $2)`, examID, refID)
	return err
}

func (r *repository) insertExamJunctions(ctx context.Context, tx pgx.Tx, examID uuid.UUID, c *Content) error {
	link := func(junction, refTable, refCol string, refID uuid.UUID) error {
		return r.linkExamJunction(ctx, tx, junction, refTable, refCol, examID, refID)
	}
	if err := link("exam_subject", "academic.subject", "subject_id", c.SubjectID); err != nil {
		return err
	}
	if err := link("exam_grade", "academic.grade", "grade_id", c.GradeID); err != nil {
		return err
	}
	if c.ChapterID != nil {
		if err := link("exam_chapter", "academic.chapter", "chapter_id", *c.ChapterID); err != nil {
			return err
		}
	}
	if c.TopicID != nil {
		if err := link("exam_topic", "academic.topic", "topic_id", *c.TopicID); err != nil {
			return err
		}
	}
	return nil
}

func (r *repository) clearExamJunctions(ctx context.Context, tx pgx.Tx, examID uuid.UUID) error {
	for _, t := range []string{"exam_subject", "exam_grade", "exam_chapter", "exam_topic"} {
		if _, err := tx.Exec(ctx, `DELETE FROM cbt.`+t+` WHERE exam_id = $1`, examID); err != nil {
			return err
		}
	}
	return nil
}

// createExamContent creates the cbt.exam master plus its academic junctions in
// one transaction. The exam_metadata row is written by CreateExam.
func packDescriptionWithBlueprint(desc string, bp map[string]interface{}) string {
	cleanDesc := desc
	if idx := strings.Index(cleanDesc, "<!--BP:"); idx != -1 {
		cleanDesc = strings.TrimSpace(cleanDesc[:idx])
	}
	if len(bp) == 0 {
		return cleanDesc
	}
	b, err := json.Marshal(bp)
	if err != nil {
		return cleanDesc
	}
	return fmt.Sprintf("%s <!--BP:%s-->", cleanDesc, string(b))
}

func unpackDescriptionWithBlueprint(rawDesc string) (string, map[string]interface{}) {
	idx := strings.Index(rawDesc, "<!--BP:")
	if idx == -1 {
		return rawDesc, nil
	}
	userDesc := strings.TrimSpace(rawDesc[:idx])
	endIdx := strings.Index(rawDesc[idx:], "-->")
	if endIdx == -1 {
		return userDesc, nil
	}
	jsonStr := rawDesc[idx+7 : idx+endIdx]
	var bp map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &bp); err != nil {
		return userDesc, nil
	}
	return userDesc, bp
}

func mapCategoryToExamType(cat string) string {
	switch strings.ToUpper(strings.TrimSpace(cat)) {
	case "UTBK_SNBT", "UTBK":
		return "UTBK"
	case "TRYOUT_NASIONAL", "TRYOUT":
		return "TRYOUT"
	case "PTS_UAS", "MID", "FINAL":
		return "MID"
	case "UJIAN_HARIAN", "UJIAN_BAB", "QUIZ":
		return "QUIZ"
	case "AKM":
		return "AKM"
	default:
		return "CBT"
	}
}

func (r *repository) createExamContent(ctx context.Context, c *Content) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	statuses, err := r.ensureExamStatuses(ctx, tx)
	if err != nil {
		return err
	}
	statusID := statuses[string(c.Status)]
	if statusID == uuid.Nil {
		statusID = statuses[string(StatusDraft)]
	}

	dbExamType := "CBT"
	if c.Metadata != nil {
		if cat, ok := c.Metadata["category"].(string); ok && cat != "" {
			dbExamType = mapCategoryToExamType(cat)
		}
	}

	packedDesc := packDescriptionWithBlueprint(c.Body, c.Metadata)
	code := "exm_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	
	metadataJSON := "{}"
	if c.Metadata != nil && len(c.Metadata) > 0 {
		b, err := json.Marshal(c.Metadata)
		if err != nil {
			metadataJSON = "{}"
		} else {
			metadataJSON = string(b)
		}
	}
	
	_, err = tx.Exec(ctx, `
		INSERT INTO cbt.exam (id, exam_code, title, description, exam_type, status_id, owner_id, created_by, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)`,
		c.ID, code, c.Title, packedDesc, dbExamType, statusID, c.CreatedBy, c.CreatedBy, metadataJSON)
	if err != nil {
		return err
	}
	if err := r.insertExamJunctions(ctx, tx, c.ID, c); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// updateExamContent updates master-level fields and junctions for a cbt.exam
// row. The metadata/randomization refresh lives in UpdateExam.
func (r *repository) updateExamContent(ctx context.Context, id uuid.UUID, req UpdateContentReq) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	statuses, err := r.ensureExamStatuses(ctx, tx)
	if err != nil {
		return err
	}
	var statusID *uuid.UUID
	if req.Status != nil {
		if sid, ok := statuses[string(*req.Status)]; ok && sid != uuid.Nil {
			statusID = &sid
		}
	}

	sets := []string{"title = COALESCE($2, title)", "status_id = COALESCE($3, status_id)", "updated_at = NOW()"}
	args := []interface{}{id, req.Title, statusID}

	// Only update description/metadata when the request actually carries them.
	// Status-only updates (approve/publish) must NOT wipe stored exam config.
	if req.Body != nil {
		packedDesc := packDescriptionWithBlueprint(*req.Body, req.Metadata)
		sets = append(sets, fmt.Sprintf("description = $%d", len(args)+1))
		args = append(args, packedDesc)
	}
	if req.Metadata != nil && len(req.Metadata) > 0 {
		b, err := json.Marshal(req.Metadata)
		if err != nil {
			return err
		}
		sets = append(sets, fmt.Sprintf("metadata = $%d::jsonb", len(args)+1))
		args = append(args, string(b))
		if cat, ok := req.Metadata["category"].(string); ok && cat != "" {
			sets = append(sets, fmt.Sprintf("exam_type = $%d", len(args)+1))
			args = append(args, mapCategoryToExamType(cat))
		}
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE cbt.exam SET %s WHERE id = $1 AND deleted_at IS NULL", strings.Join(sets, ", ")), args...); err != nil {
		return err
	}

	if req.SubjectID != nil || req.ChapterID != nil || req.TopicID != nil {
		var curGrade uuid.UUID
		_ = tx.QueryRow(ctx, `
			SELECT COALESCE(gr.grade_id, '00000000-0000-0000-0000-000000000000')
			FROM (SELECT grade_id FROM cbt.exam_grade WHERE exam_id = $1 LIMIT 1) gr`, id).Scan(&curGrade)
		if err := r.clearExamJunctions(ctx, tx, id); err != nil {
			return err
		}
		if err := r.insertExamJunctions(ctx, tx, id, &Content{
			SubjectID: ptrUUIDOrNil(req.SubjectID),
			GradeID:   curGrade,
			ChapterID: req.ChapterID,
			TopicID:   req.TopicID,
		}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (r *repository) softDeleteExam(ctx context.Context, contentID uuid.UUID) error {
	var statusCode string
	err := r.pool.QueryRow(ctx, `
		SELECT COALESCE(st.code, 'DRAFT')
		FROM cbt.exam m
		LEFT JOIN cbt.exam_status st ON st.id = m.status_id
		WHERE m.id = $1 AND m.deleted_at IS NULL`, contentID).Scan(&statusCode)
	if err != nil {
		return err
	}

	if strings.ToUpper(strings.TrimSpace(statusCode)) != "DRAFT" {
		return ErrOnlyDraftCanBeDeleted
	}

	tag, err := r.pool.Exec(ctx, `DELETE FROM cbt.exam WHERE id = $1`, contentID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func mapExamTypeToCategory(examType string) string {
	switch strings.ToUpper(strings.TrimSpace(examType)) {
	case "UTBK":
		return "UTBK_SNBT"
	case "TRYOUT":
		return "TRYOUT_NASIONAL"
	case "MID", "FINAL":
		return "PTS_UAS"
	case "QUIZ":
		return "UJIAN_HARIAN"
	case "AKM":
		return "UTBK_SNBT"
	case "CBT":
		return "UTBK_SNBT"
	default:
		if examType != "" {
			return examType
		}
		return "UTBK_SNBT"
	}
}

// examColumns projects a cbt.exam master plus its status, metadata,
// randomization, schedule and academic junctions into the ExamFull shape.
	const examColumns = `
 	m.id,
 	'EXAM'::text,
 	COALESCE(gr.grade_id, '00000000-0000-0000-0000-000000000000')::uuid,
 	COALESCE(subj.subject_id, '00000000-0000-0000-0000-000000000000')::uuid,
 	ch.chapter_id,
 	tp.topic_id,
 	NULL::uuid,
 	m.title,
 	m.description,
 	COALESCE(st.code, 'DRAFT')::text,
 	COALESCE(m.owner_id, '00000000-0000-0000-0000-000000000000')::uuid,
 	m.metadata,
 	NULL::timestamptz,
	m.created_at,
	m.updated_at,
	COALESCE(m.description, '')::text,
	COALESCE(md.duration_minute, 0),
	COALESCE(md.passing_score, 0),
	COALESCE(rz.random_question, false),
	COALESCE(rz.random_option, false),
	1::int,
	sch.start_time,
	sch.end_time,
	COALESCE(m.exam_type, 'UTBK')::text,
	COALESCE(g.name, '12 SMA / UTBK')::text,
	COALESCE(pool_cnt.cnt, 0)::int`

const examFrom = `
	FROM cbt.exam m
	LEFT JOIN cbt.exam_status st ON st.id = m.status_id
	LEFT JOIN cbt.exam_metadata md ON md.exam_id = m.id
	LEFT JOIN cbt.exam_randomization rz ON rz.exam_id = m.id
	LEFT JOIN LATERAL (SELECT start_time, end_time FROM cbt.exam_schedule WHERE exam_id = m.id ORDER BY created_at DESC LIMIT 1) sch ON true
	LEFT JOIN LATERAL (SELECT subject_id FROM cbt.exam_subject WHERE exam_id = m.id LIMIT 1) subj ON true
	LEFT JOIN LATERAL (SELECT grade_id FROM cbt.exam_grade WHERE exam_id = m.id LIMIT 1) gr ON true
	LEFT JOIN academic.grade g ON g.id = gr.grade_id
	LEFT JOIN LATERAL (SELECT chapter_id FROM cbt.exam_chapter WHERE exam_id = m.id LIMIT 1) ch ON true
	LEFT JOIN LATERAL (SELECT topic_id FROM cbt.exam_topic WHERE exam_id = m.id LIMIT 1) tp ON true
	LEFT JOIN LATERAL (
		SELECT COUNT(*)::int as cnt FROM (
			SELECT question_id FROM cbt.exam_package_question epq 
			JOIN cbt.exam_package ep ON ep.id = epq.package_id WHERE ep.exam_id = m.id
			UNION
			SELECT id as question_id FROM cbt.exam_question_pool WHERE exam_id = m.id
		) pool_q
	) pool_cnt ON true`

func scanExam(row pgx.Row) (*ExamFull, error) {
	e := &ExamFull{}
	var meta map[string]interface{}
	var rawExamType, gradeName, rawDesc string
	var totalQuestions int

	if err := row.Scan(
		&e.Content.ID, &e.Content.ContentType, &e.Content.GradeID, &e.Content.SubjectID, &e.Content.ChapterID, &e.Content.TopicID, &e.Content.LOID, &e.Content.Title, &rawDesc, &e.Content.Status, &e.Content.CreatedBy, &meta, &e.Content.PublishedAt, &e.Content.CreatedAt, &e.Content.UpdatedAt,
		&e.Exam.Description, &e.Exam.DurationMinutes, &e.Exam.PassingScore, &e.Exam.ShuffleQuestions, &e.Exam.ShuffleOptions, &e.Exam.MaxAttempts, &e.Exam.StartTime, &e.Exam.EndTime,
		&rawExamType, &gradeName, &totalQuestions,
	); err != nil {
		return nil, err
	}
	if len(meta) > 0 {
		e.Content.Metadata = meta
	}

	cleanDesc, unpackedBp := unpackDescriptionWithBlueprint(rawDesc)
	e.Content.Body = cleanDesc
	e.Exam.Description = cleanDesc

	category := mapExamTypeToCategory(rawExamType)
	defaultScoring := "IRT"
	if category == "UJIAN_HARIAN" || category == "PTS_UAS" || category == "QUIZ" || category == "MID" {
		defaultScoring = "STANDARD_POINTS"
	}

	bp := map[string]interface{}{
		"category":        category,
		"grade_level":     gradeName,
		"total_questions": totalQuestions,
		"scoring_system":  defaultScoring,
	}

	// Merge database metadata into blueprint
	for k, v := range meta {
		if v != nil && v != "" {
			bp[k] = v
		}
	}

	for k, v := range unpackedBp {
		if v != nil && v != "" {
			bp[k] = v
		}
	}

	if storedBp, ok := examCustomBlueprintStore.Load(e.Content.ID); ok {
		if bpMap, ok := storedBp.(map[string]interface{}); ok {
			for k, v := range bpMap {
				if v != nil && v != "" {
					bp[k] = v
				}
			}
		}
	}
	e.Exam.Blueprint = bp
	return e, nil
}

func (r *repository) loadExamQuestions(ctx context.Context, examID uuid.UUID) ([]ExamQuestion, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT pq.id, p.exam_id, pq.question_id, qs.subject_id, pq.question_order, pq.score, pq.created_at
		FROM cbt.exam_package_question pq
		JOIN cbt.exam_package p ON p.id = pq.package_id
		LEFT JOIN LATERAL (SELECT subject_id FROM question.question_subject WHERE question_id = pq.question_id LIMIT 1) qs ON true
		WHERE p.exam_id = $1 ORDER BY pq.question_order`, examID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []ExamQuestion
	for rows.Next() {
		var q ExamQuestion
		if err := rows.Scan(&q.ID, &q.ExamContentID, &q.QuestionContentID, &q.SubjectID, &q.DisplayOrder, &q.Points, &q.CreatedAt); err != nil {
			return nil, err
		}
		list = append(list, q)
	}
	return list, rows.Err()
}

func (r *repository) loadExamBlueprint(ctx context.Context, examID uuid.UUID) (*ExamBlueprint, error) {
	var bpID *uuid.UUID
	var createdAt *time.Time
	var n, easy, medium, hard int
	err := r.pool.QueryRow(ctx, `
		SELECT (SELECT id FROM cbt.exam_question_pool WHERE exam_id = $1 AND subject_id IS NULL ORDER BY created_at LIMIT 1),
		       MIN(created_at), COUNT(*),
		       COALESCE(SUM(total_question) FILTER (WHERE difficulty = 'EASY'), 0),
		       COALESCE(SUM(total_question) FILTER (WHERE difficulty = 'MEDIUM'), 0),
		       COALESCE(SUM(total_question) FILTER (WHERE difficulty = 'HARD'), 0)
		FROM cbt.exam_question_pool WHERE exam_id = $1 AND subject_id IS NULL`, examID).Scan(&bpID, &createdAt, &n, &easy, &medium, &hard)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	bp := &ExamBlueprint{
		ID:             *bpID,
		ExamContentID:  examID,
		EasyCount:      easy,
		MediumCount:    medium,
		HardCount:      hard,
		TotalQuestions: easy + medium + hard,
	}
	if createdAt != nil {
		bp.CreatedAt = *createdAt
	}
	return bp, nil
}

func (r *repository) CreateExam(ctx context.Context, e *Exam) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if e.Blueprint != nil {
		examCustomBlueprintStore.Store(e.ContentID, e.Blueprint)
	}

	// cbt.exam_metadata carries the authored duration/score/flags; shuffle
	// flags land on cbt.exam_randomization (max_attempts has no cbt column and
	// is dropped). Blueprint is dropped here: exam_question_pool (via the
	// explicit blueprint methods) is the source of truth.
	_, err = tx.Exec(ctx, `
		INSERT INTO cbt.exam_metadata (exam_id, duration_minute, passing_score, certificate, negative_marking, show_result, show_answer)
		VALUES ($1, $2, $3, false, $4, true, false)
		ON CONFLICT (exam_id) DO UPDATE SET
			duration_minute = CASE WHEN EXCLUDED.duration_minute > 0 THEN EXCLUDED.duration_minute ELSE cbt.exam_metadata.duration_minute END,
			passing_score = CASE WHEN EXCLUDED.passing_score > 0 THEN EXCLUDED.passing_score ELSE cbt.exam_metadata.passing_score END,
			negative_marking = EXCLUDED.negative_marking`,
		e.ContentID, e.DurationMinutes, e.PassingScore, e.NegativeMarking > 0)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO cbt.exam_randomization (exam_id, random_question, random_option)
		VALUES ($1, $2, $3)
		ON CONFLICT (exam_id) DO UPDATE SET
			random_question = EXCLUDED.random_question,
			random_option = EXCLUDED.random_option`,
		e.ContentID, e.ShuffleQuestions, e.ShuffleOptions)
	if err != nil {
		return err
	}
	// A cms.exam_packages row is only surfaced when a cbt.exam_package row
	// exists (view joins package->exam). Create the default package so the
	// exam appears in student/package catalogs immediately.
	if _, err := tx.Exec(ctx, `
		INSERT INTO cbt.exam_package (exam_id, name) VALUES ($1, 'default')
		ON CONFLICT (exam_id, name) DO NOTHING`, e.ContentID); err != nil {
		return err
	}
	// Materialize the authored question selection (blueprint subtests'
	// pool_question_ids) into cbt.exam_package_question so the CBT runtime
	// can serve them. Authoring writes the ids into the metadata jsonb only;
	// without this step the session starts empty.
	if err := r.syncExamPackageQuestions(ctx, tx, e.ContentID, e.Blueprint); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *repository) GetExam(ctx context.Context, contentID uuid.UUID) (*ExamFull, error) {
	e, err := scanExam(r.pool.QueryRow(ctx, `SELECT `+examColumns+examFrom+` WHERE m.id = $1 AND m.deleted_at IS NULL`, contentID))
	if err != nil {
		return nil, err
	}
	qs, err := r.loadExamQuestions(ctx, contentID)
	if err != nil {
		return nil, err
	}
	e.Questions = qs
	bp, err := r.loadExamBlueprint(ctx, contentID)
	if err != nil {
		return nil, err
	}
	e.TypedBlueprint = bp
	return e, nil
}

func (r *repository) UpdateExam(ctx context.Context, contentID uuid.UUID, e *Exam) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if e.Blueprint != nil {
		examCustomBlueprintStore.Store(contentID, e.Blueprint)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO cbt.exam_metadata (exam_id, duration_minute, passing_score, negative_marking)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (exam_id) DO UPDATE SET
			duration_minute = CASE WHEN EXCLUDED.duration_minute > 0 THEN EXCLUDED.duration_minute ELSE cbt.exam_metadata.duration_minute END,
			passing_score = CASE WHEN EXCLUDED.passing_score > 0 THEN EXCLUDED.passing_score ELSE cbt.exam_metadata.passing_score END,
			negative_marking = CASE WHEN $4 THEN $4 ELSE cbt.exam_metadata.negative_marking END`,
		contentID, e.DurationMinutes, e.PassingScore, e.NegativeMarking > 0)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO cbt.exam_randomization (exam_id, random_question, random_option)
		VALUES ($1, $2, $3)
		ON CONFLICT (exam_id) DO UPDATE SET
			random_question = EXCLUDED.random_question,
			random_option = EXCLUDED.random_option`,
		contentID, e.ShuffleQuestions, e.ShuffleOptions)
	if err != nil {
		return err
	}
	// Keep the authored question selection in sync (blueprint subtests'
	// pool_question_ids -> cbt.exam_package_question) so the CBT runtime can
	// serve them; authoring writes the ids into metadata jsonb only.
	if _, err := tx.Exec(ctx, `
		INSERT INTO cbt.exam_package (exam_id, name) VALUES ($1, 'default')
		ON CONFLICT (exam_id, name) DO NOTHING`, contentID); err != nil {
		return err
	}
	if err := r.syncExamPackageQuestions(ctx, tx, contentID, e.Blueprint); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *repository) DeleteExam(ctx context.Context, contentID uuid.UUID) error {
	return r.softDeleteExam(ctx, contentID)
}

func (r *repository) ListExams(ctx context.Context, filter ExamFilter) ([]ExamFull, int, error) {
	where := " WHERE m.deleted_at IS NULL"
	args := []interface{}{}
	argN := 1

	if filter.GradeID != nil && *filter.GradeID != uuid.Nil {
		// Match exact grade, ungraded exams, and exams whose grade shares the
		// same education level (legacy ListExams semantics).
		where += fmt.Sprintf(` AND (gr.grade_id = $%d OR gr.grade_id IS NULL OR gr.grade_id IN (
			SELECT g.id FROM academic.grade g
			WHERE g.education_level_id = (SELECT g2.education_level_id FROM academic.grade g2 WHERE g2.id = $%d)))`, argN, argN)
		args = append(args, *filter.GradeID)
		argN++
	}
	if filter.SubjectID != nil && *filter.SubjectID != uuid.Nil {
		where += fmt.Sprintf(" AND subj.subject_id = $%d", argN)
		args = append(args, *filter.SubjectID)
		argN++
	}
	if filter.Status != nil {
		where += fmt.Sprintf(" AND st.code = $%d", argN)
		args = append(args, string(*filter.Status))
		argN++
	}
	if filter.CreatedBy != nil {
		where += fmt.Sprintf(" AND m.owner_id = $%d", argN)
		args = append(args, *filter.CreatedBy)
		argN++
	}
	if filter.Search != "" {
		where += fmt.Sprintf(" AND (m.title ILIKE $%d OR COALESCE(m.description, '') ILIKE $%d)", argN, argN)
		args = append(args, "%"+filter.Search+"%")
		argN++
	}

	var total int
	if err := r.pool.QueryRow(ctx, "SELECT COUNT(*) "+examFrom+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 20
	}
	offset := filter.Offset

	query := fmt.Sprintf("SELECT "+examColumns+examFrom+where+
		" ORDER BY m.created_at DESC LIMIT $%d OFFSET $%d", argN, argN+1)
	args = append(args, limit, offset)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var exams []ExamFull
	for rows.Next() {
		e, err := scanExam(rows)
		if err != nil {
			return nil, 0, err
		}
		exams = append(exams, *e)
	}
	return exams, total, rows.Err()
}

// ========== EXAM QUESTIONS ==========

func (r *repository) AddExamQuestion(ctx context.Context, eq *ExamQuestion) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var packageID uuid.UUID
	if err := tx.QueryRow(ctx, `
		INSERT INTO cbt.exam_package (exam_id, name) VALUES ($1, 'default')
		ON CONFLICT (exam_id, name) DO UPDATE SET name = EXCLUDED.name
		RETURNING id`, eq.ExamContentID).Scan(&packageID); err != nil {
		return err
	}

	nextOrder := eq.DisplayOrder
	if nextOrder <= 0 {
		if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(question_order) + 1, 0) FROM cbt.exam_package_question WHERE package_id = $1`, packageID).Scan(&nextOrder); err != nil {
			return err
		}
	}

	var id uuid.UUID
	var created time.Time
	if err := tx.QueryRow(ctx, `
		INSERT INTO cbt.exam_package_question (package_id, question_id, question_order, score)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (package_id, question_id) DO UPDATE SET
			question_order = EXCLUDED.question_order,
			score = EXCLUDED.score
		RETURNING id, created_at`, packageID, eq.QuestionContentID, nextOrder, eq.Points).Scan(&id, &created); err != nil {
		return err
	}
	eq.ID = id
	eq.DisplayOrder = nextOrder
	eq.CreatedAt = created
	return tx.Commit(ctx)
}

// syncExamPackageQuestions materializes the authored question selection into
// cbt.exam_package_question for the exam's default package. The admin Studio
// stores selected questions as blueprint.subtests[].pool_question_ids in the
// exam metadata jsonb; the CBT runtime reads cbt.exam_package_question, so the
// ids must be copied over on create/update. Existing links are preserved and
// newly added ids are appended (no deletion) so the flow is additive.
func (r *repository) syncExamPackageQuestions(ctx context.Context, tx pgx.Tx, examID uuid.UUID, blueprint map[string]interface{}) error {
	if len(blueprint) == 0 {
		return nil
	}
	raw, ok := blueprint["subtests"]
	if !ok {
		return nil
	}
	subtests, ok := raw.([]interface{})
	if !ok {
		return nil
	}

	var packageID uuid.UUID
	if err := tx.QueryRow(ctx, `
		SELECT id FROM cbt.exam_package WHERE exam_id = $1 AND name = 'default'`, examID).Scan(&packageID); err != nil {
		return err
	}

	nextOrder := 0
	_ = tx.QueryRow(ctx, `SELECT COALESCE(MAX(question_order) + 1, 0) FROM cbt.exam_package_question WHERE package_id = $1`, packageID).Scan(&nextOrder)

	seen := map[uuid.UUID]bool{}
	var toInsert []uuid.UUID
	for _, st := range subtests {
		m, ok := st.(map[string]interface{})
		if !ok {
			continue
		}
		rawIDs, ok := m["pool_question_ids"]
		if !ok {
			continue
		}
		ids, ok := rawIDs.([]interface{})
		if !ok {
			continue
		}
		for _, v := range ids {
			s, ok := v.(string)
			if !ok {
				continue
			}
			id, err := uuid.Parse(s)
			if err != nil || seen[id] {
				continue
			}
			seen[id] = true
			toInsert = append(toInsert, id)
		}
	}
	if len(toInsert) == 0 {
		return nil
	}

	for _, id := range toInsert {
		if _, err := tx.Exec(ctx, `
			INSERT INTO cbt.exam_package_question (package_id, question_id, question_order, score)
			VALUES ($1, $2, $3, 0)
			ON CONFLICT (package_id, question_id) DO NOTHING`,
			packageID, id, nextOrder); err != nil {
			return err
		}
		nextOrder++
	}
	return nil
}

func (r *repository) RemoveExamQuestion(ctx context.Context, examContentID, questionContentID uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `
		DELETE FROM cbt.exam_package_question pq
		USING cbt.exam_package p
		WHERE pq.package_id = p.id AND p.exam_id = $1 AND pq.question_id = $2`, examContentID, questionContentID)
	return err
}

func (r *repository) GetExamQuestions(ctx context.Context, examContentID uuid.UUID) ([]ExamQuestion, error) {
	return r.loadExamQuestions(ctx, examContentID)
}

func (r *repository) ReorderExamQuestions(ctx context.Context, examContentID uuid.UUID, questionIDs []uuid.UUID) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for i, qID := range questionIDs {
		_, err = tx.Exec(ctx, `
			UPDATE cbt.exam_package_question pq SET question_order = $1
			FROM cbt.exam_package p
			WHERE pq.package_id = p.id AND p.exam_id = $2 AND pq.question_id = $3`, i, examContentID, qID)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// ========== EXAM BLUEPRINTS ==========

func (r *repository) CreateExamBlueprint(ctx context.Context, eb *ExamBlueprint) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Reset the exam-level pool rows (subject_id NULL) then insert one pool row
	// per difficulty bucket; exam_question_pool is the blueprint source of truth.
	if _, err := tx.Exec(ctx, `DELETE FROM cbt.exam_question_pool WHERE exam_id = $1 AND subject_id IS NULL`, eb.ExamContentID); err != nil {
		return err
	}
	buckets := []struct {
		diff string
		n    int
	}{
		{"EASY", eb.EasyCount},
		{"MEDIUM", eb.MediumCount},
		{"HARD", eb.HardCount},
	}
	for _, b := range buckets {
		if b.n <= 0 {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO cbt.exam_question_pool (exam_id, subject_id, chapter_id, difficulty, total_question)
			VALUES ($1, NULL, NULL, $2, $3)`, eb.ExamContentID, b.diff, b.n); err != nil {
			return err
		}
	}
	eb.CreatedAt = time.Now()
	return tx.Commit(ctx)
}

func (r *repository) GetExamBlueprint(ctx context.Context, examContentID uuid.UUID) (*ExamBlueprint, error) {
	bp, err := r.loadExamBlueprint(ctx, examContentID)
	if err != nil {
		return nil, err
	}
	if bp == nil {
		return nil, pgx.ErrNoRows
	}
	return bp, nil
}

// ========== EXAM PARTICIPANTS ==========

func (r *repository) AddExamParticipant(ctx context.Context, ep *ExamParticipant) error {
	var id uuid.UUID
	var created time.Time
	if err := r.pool.QueryRow(ctx, `
		INSERT INTO cbt.exam_participant (exam_id, student_id, status)
		VALUES ($1, $2, 'REGISTER')
		ON CONFLICT (exam_id, student_id) DO UPDATE SET status = EXCLUDED.status
		RETURNING id, created_at`, ep.ExamContentID, ep.UserID).Scan(&id, &created); err != nil {
		return err
	}
	ep.ID = id
	ep.CreatedAt = created
	return nil
}

func (r *repository) RemoveExamParticipant(ctx context.Context, examContentID, userID uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM cbt.exam_participant WHERE exam_id = $1 AND student_id = $2`, examContentID, userID)
	return err
}

func (r *repository) GetExamParticipants(ctx context.Context, examContentID uuid.UUID) ([]ExamParticipant, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, exam_id, student_id, created_at
		FROM cbt.exam_participant WHERE exam_id = $1 ORDER BY created_at`, examContentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var participants []ExamParticipant
	for rows.Next() {
		var ep ExamParticipant
		if err := rows.Scan(&ep.ID, &ep.ExamContentID, &ep.UserID, &ep.CreatedAt); err != nil {
			return nil, err
		}
		participants = append(participants, ep)
	}
	return participants, nil
}

// IsExamParticipant checks if a user is enrolled as a participant in an exam.
func (r *repository) IsExamParticipant(ctx context.Context, examContentID, userID uuid.UUID) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM cbt.exam_participant WHERE exam_id = $1 AND student_id = $2)
	`, examContentID, userID).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// ========== EXAM ATTEMPTS ==========

// cbtAttemptStatus and legacyAttemptStatus define the bijection between the
// legacy ExamAttemptStatus set used by the engine (IN_PROGRESS/SUBMITTED/
// GRADED/EXPIRED) and the cbt.exam_attempt.status CHECK values.
func cbtAttemptStatus(s ExamAttemptStatus) string {
	switch s {
	case AttemptInProgress:
		return "STARTED"
	case AttemptSubmitted, AttemptExpired:
		return "SUBMITTED"
	case AttemptGraded:
		return "COMPLETED"
	default:
		return "STARTED"
	}
}

func legacyAttemptStatus(cbtStatus string) ExamAttemptStatus {
	switch cbtStatus {
	case "STARTED", "READY", "REGISTERED", "PAUSED", "RESUMED":
		return AttemptInProgress
	case "SUBMITTED", "GRADING":
		return AttemptSubmitted
	case "COMPLETED":
		return AttemptGraded
	default:
		return AttemptInProgress
	}
}

// findOrCreateParticipant resolves (exam_id, student_id) to a cbt.exam_participant
// row, creating one (status REGISTER) when absent.
func (r *repository) findOrCreateParticipant(ctx context.Context, tx pgx.Tx, examID, studentID uuid.UUID) (uuid.UUID, error) {
	var pid uuid.UUID
	err := tx.QueryRow(ctx, `
		INSERT INTO cbt.exam_participant (exam_id, student_id, status)
		VALUES ($1, $2, 'REGISTER')
		ON CONFLICT (exam_id, student_id) DO NOTHING
		RETURNING id`, examID, studentID).Scan(&pid)
	if err == pgx.ErrNoRows {
		// Existing participant: return it unchanged to preserve its state
		// (READY/STARTED/FINISHED must not be reset to REGISTER on re-attempt).
		if err := tx.QueryRow(ctx, `
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

func (r *repository) CreateExamAttempt(ctx context.Context, ea *ExamAttempt) error {
	ea.ID = uuid.New()
	ea.CreatedAt = time.Now()
	if ea.Status == "" {
		ea.Status = AttemptInProgress
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	pid, err := r.findOrCreateParticipant(ctx, tx, ea.ExamContentID, ea.UserID)
	if err != nil {
		return err
	}

	attemptNo := ea.AttemptNumber
	if attemptNo <= 0 {
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(MAX(attempt_no), 0) + 1 FROM cbt.exam_attempt WHERE participant_id = $1`, pid).Scan(&attemptNo); err != nil {
			return err
		}
		ea.AttemptNumber = attemptNo
	}

	startedAt := ea.StartedAt
	if startedAt.IsZero() {
		startedAt = ea.CreatedAt
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO cbt.exam_attempt (id, participant_id, attempt_no, started_at, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		ea.ID, pid, attemptNo, startedAt, cbtAttemptStatus(ea.Status), ea.CreatedAt)
	if err != nil {
		return err
	}
	ea.StartedAt = startedAt
	return tx.Commit(ctx)
}

// scanExamAttempt projects a cbt.exam_attempt row (joined to its participant
// and optional grading_result) back into the legacy ExamAttempt shape.
func scanExamAttempt(row pgx.Row) (*ExamAttempt, error) {
	ea := &ExamAttempt{}
	var pExamID, pStudent uuid.UUID
	var status string
	var startedAt, submittedAt, gCreated *time.Time
	var score sql.NullFloat64
	if err := row.Scan(&ea.ID, &pExamID, &pStudent, &ea.AttemptNumber, &status, &startedAt, &submittedAt, &gCreated, &score, &ea.CreatedAt); err != nil {
		return nil, err
	}
	ea.ExamContentID = pExamID
	ea.UserID = pStudent
	ea.Status = legacyAttemptStatus(status)
	if startedAt != nil {
		ea.StartedAt = *startedAt
	} else {
		ea.StartedAt = ea.CreatedAt
	}
	ea.SubmittedAt = submittedAt
	if gCreated != nil {
		ea.GradedAt = gCreated
	}
	if score.Valid {
		v := score.Float64
		ea.TotalScore = &v
	}
	return ea, nil
}

const examAttemptColumns = `
	a.id, p.exam_id, p.student_id, a.attempt_no, a.status, a.started_at, a.finished_at,
	g.created_at, g.score, a.created_at`

const examAttemptFrom = `
	FROM cbt.exam_attempt a
	JOIN cbt.exam_participant p ON p.id = a.participant_id
	LEFT JOIN cbt.grading_result g ON g.attempt_id = a.id`

func (r *repository) GetExamAttempt(ctx context.Context, attemptID uuid.UUID) (*ExamAttempt, error) {
	return scanExamAttempt(r.pool.QueryRow(ctx, `SELECT `+examAttemptColumns+examAttemptFrom+` WHERE a.id = $1`, attemptID))
}

// upsertGrading writes the cbt.grading_result row for an attempt. correct/
// wrong/blank are derived from recorded student_answers against each
// question's current correct-option labels; score comes from the caller.
func (r *repository) upsertGrading(ctx context.Context, tx pgx.Tx, attemptID uuid.UUID, totalScore *float64) error {
	var score float64
	if totalScore != nil {
		score = *totalScore
	}
	// passed compares score against the exam's passing_score (defaults to pass
	// when the exam defines none). Attempt → participant → exam → metadata.
	var passingScore *float64
	err := tx.QueryRow(ctx, `
		SELECT md.passing_score
		FROM cbt.exam_attempt a
		JOIN cbt.exam_participant p ON p.id = a.participant_id
		JOIN cbt.exam_metadata md ON md.exam_id = p.exam_id
		WHERE a.id = $1`, attemptID).Scan(&passingScore)
	if err != nil && err != pgx.ErrNoRows {
		return err
	}
	passed := true
	if passingScore != nil && *passingScore > 0 {
		passed = score >= *passingScore
	}

	var total, answered, correct int
	err = tx.QueryRow(ctx, `
		WITH aq AS (
			SELECT aq.question_id, sa.selected_option
			FROM cbt.attempt_question aq
			LEFT JOIN cbt.student_answer sa ON sa.attempt_question_id = aq.id
			WHERE aq.attempt_id = $1
		)
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE selected_option IS NOT NULL AND selected_option <> ''),
			COUNT(*) FILTER (WHERE selected_option IS NOT NULL AND selected_option <> ''
				AND selected_option = (
					SELECT string_agg(op.label, ',' ORDER BY op.display_order)
					FROM question.question q
					JOIN question.question_option op ON op.question_version_id = q.current_version_id AND op.is_correct
					WHERE q.id = aq.question_id))
		FROM aq`, attemptID).Scan(&total, &answered, &correct)
	if err != nil {
		return err
	}
	wrong := answered - correct
	blank := total - answered
	_, err = tx.Exec(ctx, `
		INSERT INTO cbt.grading_result (attempt_id, score, correct, wrong, blank, passed)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (attempt_id) DO UPDATE SET
			score = EXCLUDED.score,
			correct = EXCLUDED.correct,
			wrong = EXCLUDED.wrong,
			blank = EXCLUDED.blank,
			passed = EXCLUDED.passed`,
		attemptID, score, correct, wrong, blank, passed)
	return err
}

func (r *repository) UpdateExamAttempt(ctx context.Context, ea *ExamAttempt) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		UPDATE cbt.exam_attempt
		SET status = $1, finished_at = COALESCE($2, finished_at), updated_at = NOW()
		WHERE id = $3`,
		cbtAttemptStatus(ea.Status), ea.SubmittedAt, ea.ID)
	if err != nil {
		return err
	}

	// GRADED carries the grading_result write; other transitions (e.g. the
	// SUBMITTED step of SubmitAttempt) leave grading untouched.
	if ea.Status == AttemptGraded {
		if err := r.upsertGrading(ctx, tx, ea.ID, ea.TotalScore); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (r *repository) GetUserExamAttempts(ctx context.Context, examContentID, userID uuid.UUID) ([]ExamAttempt, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT `+examAttemptColumns+examAttemptFrom+`
		WHERE p.exam_id = $1 AND p.student_id = $2
		ORDER BY a.attempt_no`, examContentID, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var attempts []ExamAttempt
	for rows.Next() {
		ea, err := scanExamAttempt(rows)
		if err != nil {
			return nil, err
		}
		attempts = append(attempts, *ea)
	}
	return attempts, rows.Err()
}

// ========== EXAM ANSWERS ==========

// optionIDsToLabels resolves a set of question_option ids into a
// display_order-sorted comma-label string, matching the string_agg used for
// grading so a recorded answer round-trips to the correct-option comparison.
func (r *repository) optionIDsToLabels(ctx context.Context, tx pgx.Tx, ids []uuid.UUID) (string, error) {
	filtered := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		if id != uuid.Nil {
			filtered = append(filtered, id)
		}
	}
	if len(filtered) == 0 {
		return "", nil
	}
	var labels string
	if err := tx.QueryRow(ctx, `
		SELECT string_agg(op.label, ',' ORDER BY op.display_order)
		FROM question.question_option op WHERE op.id = ANY($1)`, filtered).Scan(&labels); err != nil {
		return "", err
	}
	return labels, nil
}

// ensureAttemptQuestion resolves (attempt_id, question_id) to its
// cbt.attempt_question row, creating it (next display_order) when absent.
func (r *repository) ensureAttemptQuestion(ctx context.Context, tx pgx.Tx, attemptID, questionID uuid.UUID) (uuid.UUID, error) {
	var id uuid.UUID
	err := tx.QueryRow(ctx, `SELECT id FROM cbt.attempt_question WHERE attempt_id = $1 AND question_id = $2`, attemptID, questionID).Scan(&id)
	if err == nil {
		return id, nil
	}
	if err != pgx.ErrNoRows {
		return uuid.Nil, err
	}
	var next int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(display_order), 0) + 1 FROM cbt.attempt_question WHERE attempt_id = $1`, attemptID).Scan(&next); err != nil {
		return uuid.Nil, err
	}
	id = uuid.New()
	if _, err := tx.Exec(ctx, `
		INSERT INTO cbt.attempt_question (id, attempt_id, question_id, display_order)
		VALUES ($1, $2, $3, $4)`, id, attemptID, questionID, next); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

func (r *repository) upsertStudentAnswer(ctx context.Context, tx pgx.Tx, ea *ExamAnswer, aqID uuid.UUID, labels string) error {
	answeredAt := ea.CreatedAt
	if answeredAt.IsZero() {
		answeredAt = time.Now()
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO cbt.student_answer (id, attempt_question_id, selected_option, answered_at)
		VALUES ($1, $2, NULLIF($3, ''), $4)
		ON CONFLICT (attempt_question_id) DO UPDATE SET
			selected_option = NULLIF(EXCLUDED.selected_option, ''),
			answered_at = EXCLUDED.answered_at`,
		ea.ID, aqID, labels, answeredAt)
	return err
}

func (r *repository) CreateExamAnswer(ctx context.Context, ea *ExamAnswer) error {
	ea.ID = uuid.New()
	ea.CreatedAt = time.Now()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	aqID, err := r.ensureAttemptQuestion(ctx, tx, ea.AttemptID, ea.QuestionContentID)
	if err != nil {
		return err
	}
	labels, err := r.optionIDsToLabels(ctx, tx, ea.SelectedOptions)
	if err != nil {
		return err
	}
	if err := r.upsertStudentAnswer(ctx, tx, ea, aqID, labels); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *repository) GetExamAnswers(ctx context.Context, attemptID uuid.UUID) ([]ExamAnswer, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT sa.id, aq.attempt_id, aq.question_id, sa.selected_option, sa.answered_at
		FROM cbt.student_answer sa
		JOIN cbt.attempt_question aq ON aq.id = sa.attempt_question_id
		WHERE aq.attempt_id = $1
		ORDER BY aq.display_order`, attemptID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var answers []ExamAnswer
	var labels *string
	for rows.Next() {
		var ea ExamAnswer
		if err := rows.Scan(&ea.ID, &ea.AttemptID, &ea.QuestionContentID, &labels, &ea.CreatedAt); err != nil {
			return nil, err
		}
		if labels != nil && *labels != "" {
			ids, err := r.labelsToOptionIDs(ctx, ea.QuestionContentID, *labels)
			if err != nil {
				return nil, err
			}
			ea.SelectedOptions = ids
		}
		answers = append(answers, ea)
	}
	return answers, rows.Err()
}

// labelsToOptionIDs reverses a stored selected_option label string back into
// the question's option uuids (only those still present on the current version).
func (r *repository) labelsToOptionIDs(ctx context.Context, questionID uuid.UUID, labels string) ([]uuid.UUID, error) {
	split := strings.Split(labels, ",")
	var ids []uuid.UUID
	rows, err := r.pool.Query(ctx, `
		SELECT op.id FROM question.question q
		JOIN question.question_option op ON op.question_version_id = q.current_version_id
		WHERE q.id = $1 AND op.label = ANY($2)
		ORDER BY op.display_order`, questionID, split)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *repository) UpdateExamAnswer(ctx context.Context, ea *ExamAnswer) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	labels, err := r.optionIDsToLabels(ctx, tx, ea.SelectedOptions)
	if err != nil {
		return err
	}
	answeredAt := time.Now()
	_, err = tx.Exec(ctx, `
		UPDATE cbt.student_answer SET selected_option = NULLIF($2, ''), answered_at = $3 WHERE id = $1`,
		ea.ID, labels, answeredAt)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *repository) BatchCreateExamAnswers(ctx context.Context, answers []ExamAnswer) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, ea := range answers {
		ea.ID = uuid.New()
		ea.CreatedAt = time.Now()
		aqID, err := r.ensureAttemptQuestion(ctx, tx, ea.AttemptID, ea.QuestionContentID)
		if err != nil {
			return err
		}
		labels, err := r.optionIDsToLabels(ctx, tx, ea.SelectedOptions)
		if err != nil {
			return err
		}
		if err := r.upsertStudentAnswer(ctx, tx, &ea, aqID, labels); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// ========== QUESTION POOLS ==========

// QuestionPool is backed by cbt.exam_question_pool difficulty-distribution
// rows. The legacy content_question_pools table no longer exists; percentages
// are computed from the stored per-difficulty totals on read.

// poolBuckets converts the pool's percentage split into concrete per-difficulty
// counts for persistence.
func poolBuckets(pool *QuestionPool) []struct {
	diff  string
	count int
} {
	easy := pool.TotalPoolSize * pool.EasyPct / 100
	medium := pool.TotalPoolSize * pool.MediumPct / 100
	hard := pool.TotalPoolSize - easy - medium
	return []struct {
		diff  string
		count int
	}{
		{"EASY", easy},
		{"MEDIUM", medium},
		{"HARD", hard},
	}
}

func (r *repository) CreateQuestionPool(ctx context.Context, qp *QuestionPool) error {
	qp.ID = uuid.New()
	qp.CreatedAt = time.Now()
	qp.UpdatedAt = time.Now()
	if qp.ExamContentID == nil {
		return fmt.Errorf("question pool requires exam_content_id")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Reset the exam's rows for this subject then insert one pool row per
	// difficulty bucket.
	if _, err := tx.Exec(ctx, `DELETE FROM cbt.exam_question_pool WHERE exam_id = $1 AND subject_id = $2`, *qp.ExamContentID, qp.SubjectID); err != nil {
		return err
	}
	for _, b := range poolBuckets(qp) {
		if b.count <= 0 {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO cbt.exam_question_pool (id, exam_id, subject_id, difficulty, total_question)
			VALUES ($1, $2, $3, $4, $5)`, uuid.New(), *qp.ExamContentID, qp.SubjectID, b.diff, b.count); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (r *repository) GetQuestionPool(ctx context.Context, examContentID uuid.UUID) (*QuestionPool, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, subject_id, chapter_id, difficulty, total_question
		FROM cbt.exam_question_pool WHERE exam_id = $1`, examContentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type pr struct {
		id         uuid.UUID
		subject    uuid.UUID
		chapter    *uuid.UUID
		difficulty string
		total      int
	}
	var prs []pr
	for rows.Next() {
		var p pr
		if err := rows.Scan(&p.id, &p.subject, &p.chapter, &p.difficulty, &p.total); err != nil {
			return nil, err
		}
		prs = append(prs, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(prs) == 0 {
		return nil, pgx.ErrNoRows
	}

	qp := &QuestionPool{
		ID:            prs[0].id,
		ExamContentID: &examContentID,
		SubjectID:     prs[0].subject,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
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
		SELECT random_question, random_option FROM cbt.exam_randomization WHERE exam_id = $1`, examContentID).Scan(&shuffleQ, &shuffleO)
	qp.ShuffleQuestions = shuffleQ != nil && *shuffleQ
	qp.ShuffleOptions = shuffleO != nil && *shuffleO
	return qp, nil
}

func (r *repository) UpdateQuestionPool(ctx context.Context, qp *QuestionPool) error {
	if qp.ExamContentID == nil {
		return fmt.Errorf("question pool requires exam_content_id")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM cbt.exam_question_pool WHERE exam_id = $1 AND subject_id = $2`, *qp.ExamContentID, qp.SubjectID); err != nil {
		return err
	}
	for _, b := range poolBuckets(qp) {
		if b.count <= 0 {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO cbt.exam_question_pool (id, exam_id, subject_id, difficulty, total_question)
			VALUES ($1, $2, $3, $4, $5)`, uuid.New(), *qp.ExamContentID, qp.SubjectID, b.diff, b.count); err != nil {
			return err
		}
	}
	qp.UpdatedAt = time.Now()
	return tx.Commit(ctx)
}

func (r *repository) DeleteQuestionPool(ctx context.Context, id uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM cbt.exam_question_pool WHERE id=$1`, id)
	return err
}

func (r *repository) GetQuestionsForPool(ctx context.Context, pool *QuestionPool) ([]uuid.UUID, error) {
	// Selection runs against published question.question rows linked to the
	// pool's subject (+ optional chapters) via the academic junctions, split
	// by the difficulty distribution in question_metadata.
	from := `
		FROM question.question q
		JOIN question.question_metadata m ON m.question_id = q.id
		WHERE q.deleted_at IS NULL
		  AND q.status_id = (SELECT id FROM question.question_status WHERE code = 'PUBLISHED')
		  AND EXISTS (SELECT 1 FROM question.question_subject qs WHERE qs.question_id = q.id AND qs.subject_id = $1)`
	args := []interface{}{pool.SubjectID}
	argN := 2

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
		{"EASY", easyCount},
		{"MEDIUM", mediumCount},
		{"HARD", hardCount},
	}

	var allIDs []uuid.UUID
	for _, b := range buckets {
		if b.count <= 0 {
			continue
		}
		rows, err := r.pool.Query(ctx,
			fmt.Sprintf(`SELECT q.id %s AND m.difficulty_level = $%d ORDER BY RANDOM() LIMIT $%d`, from, argN, argN+1),
			append(append([]interface{}{}, args...), b.diff, b.count)...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			allIDs = append(allIDs, id)
		}
		rows.Close()
	}
	return allIDs, nil
}

// ========== EXAM SESSION QUESTIONS ==========

// AddSessionQuestions snapshots the given questions into cbt.attempt_question
// (+ cbt.attempt_option labels) for the session/attempt, mirroring the runtime.
// The engine passes the cbt.exam_attempt id as sessionID.
func (r *repository) AddSessionQuestions(ctx context.Context, sessionID uuid.UUID, questionIDs []uuid.UUID, shuffleQuestions, shuffleOptions bool) error {
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

// ========== EXAM ANALYTICS ==========

func (r *repository) GetExamAnalytics(ctx context.Context, examContentID uuid.UUID) (*ExamAnalytics, error) {
	a := &ExamAnalytics{}

	_ = r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM cbt.exam_participant WHERE exam_id = $1`, examContentID).Scan(&a.TotalParticipants)
	_ = r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM cbt.exam_attempt a JOIN cbt.exam_participant p ON p.id = a.participant_id WHERE p.exam_id = $1`, examContentID).Scan(&a.TotalStarted)
	_ = r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM cbt.exam_attempt a JOIN cbt.exam_participant p ON p.id = a.participant_id WHERE p.exam_id = $1 AND a.status = 'COMPLETED'`, examContentID).Scan(&a.TotalFinished)

	var passed, total int
	rows, err := r.pool.Query(ctx, `
		SELECT g.passed
		FROM cbt.exam_attempt a
		JOIN cbt.exam_participant p ON p.id = a.participant_id
		JOIN cbt.grading_result g ON g.attempt_id = a.id
		WHERE p.exam_id = $1`, examContentID)
	if err == nil {
		for rows.Next() {
			var ok bool
			if err := rows.Scan(&ok); err != nil {
				continue
			}
			total++
			if ok {
				passed++
			}
		}
		rows.Close()
	}

	_ = r.pool.QueryRow(ctx, `SELECT COALESCE(ROUND(AVG(g.score)::numeric, 2), 0) FROM cbt.grading_result g
		JOIN cbt.exam_attempt a ON a.id = g.attempt_id
		JOIN cbt.exam_participant p ON p.id = a.participant_id WHERE p.exam_id = $1`, examContentID).Scan(&a.AverageScore)
	_ = r.pool.QueryRow(ctx, `SELECT COALESCE(MAX(g.score), 0) FROM cbt.grading_result g
		JOIN cbt.exam_attempt a ON a.id = g.attempt_id
		JOIN cbt.exam_participant p ON p.id = a.participant_id WHERE p.exam_id = $1`, examContentID).Scan(&a.HighestScore)
	_ = r.pool.QueryRow(ctx, `SELECT COALESCE(MIN(g.score), 0) FROM cbt.grading_result g
		JOIN cbt.exam_attempt a ON a.id = g.attempt_id
		JOIN cbt.exam_participant p ON p.id = a.participant_id WHERE p.exam_id = $1`, examContentID).Scan(&a.LowestScore)
	if total > 0 {
		a.PassRate = float64(passed) / float64(total) * 100
	}
	return a, nil
}

// ========== EXAM SUBJECT BLUEPRINTS ==========

func (r *repository) CreateExamSubjectBlueprint(ctx context.Context, esb *ExamSubjectBlueprint) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM cbt.exam_question_pool WHERE exam_id = $1 AND subject_id = $2`, esb.ExamContentID, esb.SubjectID); err != nil {
		return err
	}
	buckets := []struct {
		diff string
		n    int
	}{
		{"EASY", esb.EasyCount},
		{"MEDIUM", esb.MediumCount},
		{"HARD", esb.HardCount},
	}
	for _, b := range buckets {
		if b.n <= 0 {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO cbt.exam_question_pool (exam_id, subject_id, difficulty, total_question)
			VALUES ($1, $2, $3, $4)`, esb.ExamContentID, esb.SubjectID, b.diff, b.n); err != nil {
			return err
		}
	}
	esb.CreatedAt = time.Now()
	return tx.Commit(ctx)
}

func (r *repository) GetExamSubjectBlueprints(ctx context.Context, examContentID uuid.UUID) ([]ExamSubjectBlueprint, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT (SELECT id FROM cbt.exam_question_pool p2 WHERE p2.exam_id = $1 AND p2.subject_id = p1.subject_id ORDER BY p2.created_at LIMIT 1),
		       MIN(p1.created_at), p1.subject_id,
		       COALESCE(SUM(p1.total_question) FILTER (WHERE p1.difficulty = 'EASY'), 0),
		       COALESCE(SUM(p1.total_question) FILTER (WHERE p1.difficulty = 'MEDIUM'), 0),
		       COALESCE(SUM(p1.total_question) FILTER (WHERE p1.difficulty = 'HARD'), 0)
		FROM cbt.exam_question_pool p1
		WHERE p1.exam_id = $1 AND p1.subject_id IS NOT NULL
		GROUP BY p1.subject_id ORDER BY p1.subject_id`, examContentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var blueprints []ExamSubjectBlueprint
	for rows.Next() {
		var b ExamSubjectBlueprint
		if err := rows.Scan(&b.ID, &b.CreatedAt, &b.SubjectID, &b.EasyCount, &b.MediumCount, &b.HardCount); err != nil {
			return nil, err
		}
		b.ExamContentID = examContentID
		b.TotalQuestions = b.EasyCount + b.MediumCount + b.HardCount
		blueprints = append(blueprints, b)
	}
	return blueprints, rows.Err()
}

func (r *repository) DeleteExamSubjectBlueprint(ctx context.Context, examContentID, subjectID uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM cbt.exam_question_pool WHERE exam_id=$1 AND subject_id=$2`, examContentID, subjectID)
	return err
}

// ========== PRACTICE SETS ==========

// The content.practice_set layout (migration 072) has a master+question stem
// but no per-set config columns (they belong to Batch 3's practice runtime).
// ponytail: key the set by the material content id, pack practice_content_id
// into title and questions_count/time_limit into description (JSON) so the
// PracticeSet struct round-trips until a practice-set runtime lands.

type practiceSetMeta struct {
	QuestionsCount   int `json:"questions_count"`
	TimeLimitSeconds int `json:"time_limit_seconds"`
}

func (r *repository) CreatePracticeSet(ctx context.Context, ps *PracticeSet) error {
	if ps.ID == uuid.Nil {
		ps.ID = ps.ContentID
	}
	ps.CreatedAt = time.Now()
	meta, err := json.Marshal(practiceSetMeta{QuestionsCount: ps.QuestionsCount, TimeLimitSeconds: ps.TimeLimitSeconds})
	if err != nil {
		return err
	}
	code := "ps_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	_, err = r.pool.Exec(ctx, `
		INSERT INTO content.practice_set (id, practice_set_code, title, description, created_by, created_at)
		VALUES ($1, $2, $3, $4, NULL, $5)`,
		ps.ID, code, ps.PracticeContentID.String(), string(meta), ps.CreatedAt)
	return err
}

func scanPracticeSet(row pgx.Row) (*PracticeSet, error) {
	ps := &PracticeSet{}
	var description string
	if err := row.Scan(&ps.ID, &ps.PracticeContentID, &description, &ps.CreatedAt); err != nil {
		return nil, err
	}
	ps.ContentID = ps.ID
	var meta practiceSetMeta
	if json.Unmarshal([]byte(description), &meta) == nil {
		ps.QuestionsCount = meta.QuestionsCount
		ps.TimeLimitSeconds = meta.TimeLimitSeconds
	}
	return ps, nil
}

func (r *repository) GetPracticeSet(ctx context.Context, contentID uuid.UUID) (*PracticeSet, error) {
	return scanPracticeSet(r.pool.QueryRow(ctx, `
		SELECT id, title, description, created_at
		FROM content.practice_set
		WHERE id = $1 AND deleted_at IS NULL`, contentID))
}

func (r *repository) GetPracticeSetByPracticeContent(ctx context.Context, practiceContentID uuid.UUID) (*PracticeSet, error) {
	return scanPracticeSet(r.pool.QueryRow(ctx, `
		SELECT id, title, description, created_at
		FROM content.practice_set
		WHERE title = $1 AND deleted_at IS NULL
		ORDER BY created_at LIMIT 1`, practiceContentID.String()))
}

func (r *repository) DeletePracticeSet(ctx context.Context, contentID uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM content.practice_set WHERE id = $1`, contentID)
	return err
}

// ========== PRACTICE SESSIONS ==========

// Practice sessions persist to content.practice_session (migration 211). The
// schema carries student/subject/grade, status, scores and timestamps but has
// no practice_set_id, tag_filter, time_spent_seconds or subject_breakdown
// columns, so those DTO fields are dropped on write and nil on read.

// practiceCBTStatus maps the DTO status into the practice_session CHECK domain.
func practiceCBTStatus(s PracticeSessionStatus) string {
	switch s {
	case PracticeSubmitted:
		return "SUBMITTED"
	case PracticeGraded:
		return "GRADED"
	default:
		return "IN_PROGRESS"
	}
}

func legacyPracticeStatus(s string) PracticeSessionStatus {
	switch s {
	case "SUBMITTED":
		return PracticeSubmitted
	case "GRADED":
		return PracticeGraded
	default:
		return PracticeInProgress
	}
}

func floatPtr(f float64) *float64 { return &f }

func (r *repository) CreatePracticeSession(ctx context.Context, ps *PracticeSession) error {
	ps.ID = uuid.New()
	ps.CreatedAt = time.Now()
	ps.StartedAt = time.Now()
	status := practiceCBTStatus(ps.Status)
	_, err := r.pool.Exec(ctx, `
		INSERT INTO content.practice_session (id, student_id, subject_id, grade_id, status, started_at, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
	`, ps.ID, ps.UserID, ps.SubjectID, ps.GradeID, status, ps.StartedAt, ps.CreatedAt)
	return err
}

func scanPracticeSession(row pgx.Row) (*PracticeSession, error) {
	ps := &PracticeSession{}
	var status string
	var total, max float64
	var finishedAt *time.Time
	if err := row.Scan(&ps.ID, &ps.UserID, &ps.SubjectID, &ps.GradeID, &status, &total, &max, &ps.StartedAt, &finishedAt, &ps.CreatedAt); err != nil {
		return nil, err
	}
	ps.Status = legacyPracticeStatus(status)
	ps.TotalScore = floatPtr(total)
	ps.MaxScore = floatPtr(max)
	if finishedAt != nil {
		if ps.Status == PracticeGraded {
			ps.GradedAt = finishedAt
		} else if ps.Status == PracticeSubmitted {
			ps.SubmittedAt = finishedAt
		}
	}
	return ps, nil
}

const practiceSessionColumns = `
	id, student_id, subject_id, grade_id, status, total_score, max_score, started_at, finished_at, created_at`

func (r *repository) GetPracticeSession(ctx context.Context, sessionID uuid.UUID) (*PracticeSession, error) {
	return scanPracticeSession(r.pool.QueryRow(ctx, `
		SELECT `+practiceSessionColumns+`
		FROM content.practice_session WHERE id = $1`, sessionID))
}

func (r *repository) UpdatePracticeSession(ctx context.Context, ps *PracticeSession) error {
	// content.practice_session has a single finished_at timestamp; persist the
	// later of submitted/graded so the grade state is never lost.
	var finishedAt *time.Time
	switch {
	case ps.GradedAt != nil:
		finishedAt = ps.GradedAt
	case ps.SubmittedAt != nil:
		finishedAt = ps.SubmittedAt
	case ps.Status == PracticeGraded || ps.Status == PracticeSubmitted:
		now := time.Now()
		finishedAt = &now
	}
	_, err := r.pool.Exec(ctx, `
		UPDATE content.practice_session SET
			status = $2, finished_at = COALESCE($3, finished_at),
			total_score = COALESCE($4, total_score), max_score = COALESCE($5, max_score),
			updated_at = NOW()
		WHERE id = $1
	`, ps.ID, practiceCBTStatus(ps.Status), finishedAt, ps.TotalScore, ps.MaxScore)
	return err
}

func (r *repository) GetUserPracticeSessions(ctx context.Context, userID uuid.UUID, limit, offset int) ([]PracticeSession, int, error) {
	var total int
	if err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM content.practice_session WHERE student_id=$1`, userID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.pool.Query(ctx, `
		SELECT `+practiceSessionColumns+`
		FROM content.practice_session WHERE student_id=$1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`, userID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var sessions []PracticeSession
	for rows.Next() {
		ps, err := scanPracticeSession(rows)
		if err != nil {
			return nil, 0, err
		}
		sessions = append(sessions, *ps)
	}
	return sessions, total, rows.Err()
}

// ========== PRACTICE QUESTION SELECTION ==========

func (r *repository) GetQuestionsForPractice(ctx context.Context, subjectID, gradeID *uuid.UUID, tagFilter map[string]interface{}, count int) ([]uuid.UUID, error) {
	from := `
		FROM question.question q
		WHERE q.deleted_at IS NULL
		  AND q.status_id = (SELECT id FROM question.question_status WHERE code = 'PUBLISHED')`
	args := []interface{}{}
	argN := 1

	if subjectID != nil {
		from += fmt.Sprintf(` AND EXISTS (SELECT 1 FROM question.question_subject qs WHERE qs.question_id = q.id AND qs.subject_id = $%d)`, argN)
		args = append(args, *subjectID)
		argN++
	}
	if gradeID != nil {
		from += fmt.Sprintf(` AND EXISTS (SELECT 1 FROM question.question_grade qg WHERE qg.question_id = q.id AND qg.grade_id = $%d)`, argN)
		args = append(args, *gradeID)
		argN++
	}
	// ponytail: tag_filter (question.tag) is not wired to an exam/practice
	// filter yet; it was a no-op in the legacy query too. add when practice
	// runtime (Batch 3) lands.
	_ = tagFilter

	args = append(args, count)
	rows, err := r.pool.Query(ctx, fmt.Sprintf(`SELECT q.id %s ORDER BY RANDOM() LIMIT $%d`, from, argN), args...)
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
	return ids, nil
}

func (r *repository) GetQuestionsForMaterialPractice(ctx context.Context, materialContentID uuid.UUID, count int) ([]uuid.UUID, error) {
	// Questions authored against a material are linked via
	// question.question_learning_material.material_id; those are the practice
	// candidates, restricted to published rows.
	rows, err := r.pool.Query(ctx, `
		SELECT lm.question_id
		FROM question.question_learning_material lm
		JOIN question.question q ON q.id = lm.question_id
		WHERE lm.material_id = $1 AND q.deleted_at IS NULL
		  AND q.status_id = (SELECT id FROM question.question_status WHERE code = 'PUBLISHED')
		ORDER BY q.created_at DESC
		LIMIT $2`, materialContentID, count)
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
	return ids, nil
}

// ========== PRACTICE QUESTION SELECTION (Extended) ==========

// AddSessionQuestionsWithSubject snapshots questions into cbt.attempt_question
// (+ cbt.attempt_option) for the session/attempt, the same as AddSessionQuestions
// (cbt.attempt_question carries no per-question subject column; subject scoring
// resolves subjects on the fly via GetQuestionSubject).
func (r *repository) AddSessionQuestionsWithSubject(ctx context.Context, sessionID uuid.UUID, questionIDs []uuid.UUID, shuffleQuestions, shuffleOptions bool) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for i, qid := range questionIDs {
		aqID := uuid.New()
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

func (r *repository) GetSubjectName(ctx context.Context, subjectID uuid.UUID) (string, error) {
	var name string
	err := r.pool.QueryRow(ctx, `SELECT name FROM academic.subject WHERE id = $1`, subjectID).Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
}

func (r *repository) FindSubjectIDByName(ctx context.Context, name string) (*uuid.UUID, error) {
	var id uuid.UUID
	err := r.pool.QueryRow(ctx, `SELECT id FROM academic.subject WHERE name ILIKE $1 OR code ILIKE $1 LIMIT 1`, strings.TrimSpace(name)).Scan(&id)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func (r *repository) FindGradeIDByName(ctx context.Context, name string) (*uuid.UUID, error) {
	var id uuid.UUID
	err := r.pool.QueryRow(ctx, `SELECT id FROM academic.grade WHERE name ILIKE $1 OR code ILIKE $1 LIMIT 1`, strings.TrimSpace(name)).Scan(&id)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func (r *repository) GetQuestionSubject(ctx context.Context, questionContentID uuid.UUID) (uuid.UUID, error) {
	var subjectID uuid.UUID
	err := r.pool.QueryRow(ctx, `
		SELECT subject_id FROM question.question_subject WHERE question_id = $1 LIMIT 1`, questionContentID).Scan(&subjectID)
	if err != nil {
		return uuid.Nil, err
	}
	return subjectID, nil
}
