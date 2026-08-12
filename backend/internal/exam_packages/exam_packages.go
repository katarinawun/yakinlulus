package exam_packages

import (
	"context"
	"errors"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"yakinlulus.id/backend/internal/middleware"
	"yakinlulus.id/backend/internal/shared"
)

// ---- Models ----

type ExamPackage struct {
	ID              uuid.UUID  `json:"id"`
	Code            string     `json:"code"`
	Name            string     `json:"name"`
	EducationLevel  string     `json:"education_level"`
	GradeID         *uuid.UUID `json:"grade_id,omitempty"`
	IsActive        bool       `json:"is_active"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	SubjectsCount   int        `json:"subjects_count"`
	TotalQuestions  int        `json:"total_questions"`
	DurationMinutes int        `json:"duration_minutes"`
}

type PackageExam struct {
	ExamContentID uuid.UUID `json:"exam_content_id"`
	SubjectID     uuid.UUID `json:"subject_id"`
	SubjectName   string    `json:"subject_name"`
	DisplayOrder  int       `json:"display_order"`
}

type SavePackageRequest struct {
	Code           string `json:"code"`
	Name           string `json:"name"`
	EducationLevel string `json:"education_level"`
	GradeID        *string `json:"grade_id,omitempty"`
	IsActive       *bool   `json:"is_active,omitempty"`
}

type LinkExamRequest struct {
	ExamContentID string `json:"exam_content_id"`
	SubjectID     string `json:"subject_id"`
	DisplayOrder  int    `json:"display_order"`
}

func validateSaveRequest(req SavePackageRequest) error {
	if req.Code == "" {
		return fiber.NewError(fiber.StatusBadRequest, "code required")
	}
	if req.Name == "" {
		return fiber.NewError(fiber.StatusBadRequest, "name required")
	}
	switch req.EducationLevel {
	case "SD", "SMP", "SMA", "UNIVERSITY":
	default:
		return fiber.NewError(fiber.StatusBadRequest, "education_level must be SD/SMP/SMA/UNIVERSITY")
	}
	if req.GradeID != nil && *req.GradeID != "" {
		if _, err := uuid.Parse(*req.GradeID); err != nil {
			return fiber.NewError(fiber.StatusBadRequest, "invalid grade_id")
		}
	}
	return nil
}

func validateLinkRequest(req LinkExamRequest) error {
	if _, err := uuid.Parse(req.ExamContentID); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid exam_content_id")
	}
	if _, err := uuid.Parse(req.SubjectID); err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "invalid subject_id")
	}
	return nil
}

// ---- Repository ----

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

const pkgCols = `v.id, v.code, v.name, v.education_level, v.grade_id, v.is_active, v.created_at, v.updated_at,
	(SELECT COUNT(*) FROM cms.exam_package_exams pe WHERE pe.package_id = v.id) AS subjects_count,
	(SELECT COUNT(*) FROM cbt.exam_package_question pq JOIN cbt.exam_package p ON p.id = pq.package_id WHERE p.exam_id = ep.exam_id) AS total_questions,
	COALESCE((SELECT duration_minute FROM cbt.exam_metadata md WHERE md.exam_id = ep.exam_id), 0) AS duration_minutes`

func scanPackage(row pgx.Row) (*ExamPackage, error) {
	var p ExamPackage
	var gradeID *uuid.UUID
	err := row.Scan(&p.ID, &p.Code, &p.Name, &p.EducationLevel, &gradeID, &p.IsActive, &p.CreatedAt, &p.UpdatedAt, &p.SubjectsCount, &p.TotalQuestions, &p.DurationMinutes)
	if err != nil {
		return nil, err
	}
	p.GradeID = gradeID
	return &p, nil
}

func (r *Repository) List(ctx context.Context, level string) ([]ExamPackage, error) {
	query := `SELECT ` + pkgCols + ` FROM cms.exam_packages v JOIN cbt.exam_package ep ON ep.id = v.id`
	args := []interface{}{}
	if level != "" {
		query += ` WHERE v.education_level = $1`
		args = append(args, level)
	}
	query += ` ORDER BY v.name`
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ExamPackage
	for rows.Next() {
		p, err := scanPackage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

func (r *Repository) GetByID(ctx context.Context, id uuid.UUID) (*ExamPackage, error) {
	return scanPackage(r.pool.QueryRow(ctx, `SELECT `+pkgCols+` FROM cms.exam_packages v JOIN cbt.exam_package ep ON ep.id = v.id WHERE v.id = $1`, id))
}

func (r *Repository) Create(ctx context.Context, req SavePackageRequest) (*ExamPackage, error) {
	active := true
	if req.IsActive != nil {
		active = *req.IsActive
	}
	var gradeID *uuid.UUID
	if req.GradeID != nil && *req.GradeID != "" {
		if id, err := uuid.Parse(*req.GradeID); err == nil {
			gradeID = &id
		}
	}
	examID := uuid.New()
	if _, err := r.pool.Exec(ctx, `
		INSERT INTO cbt.exam (id, exam_code, title, description, exam_type, created_at, updated_at)
		VALUES ($1, $2, $3, $4, 'CBT', NOW(), NOW())`,
		examID, req.Code, req.Name, req.Name); err != nil {
		return nil, err
	}
	if gradeID != nil {
		if _, err := r.pool.Exec(ctx,
			`INSERT INTO cbt.exam_grade (exam_id, grade_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			examID, *gradeID); err != nil {
			return nil, err
		}
	}
	var id uuid.UUID
	err := r.pool.QueryRow(ctx,
		`INSERT INTO cbt.exam_package (exam_id, name, is_active) VALUES ($1, $2, $3) RETURNING id`,
		examID, req.Name, active).Scan(&id)
	if err != nil {
		return nil, err
	}
	return r.GetByID(ctx, id)
}

func (r *Repository) Update(ctx context.Context, id uuid.UUID, req SavePackageRequest) (*ExamPackage, error) {
	var examID uuid.UUID
	if err := r.pool.QueryRow(ctx,
		`SELECT exam_id FROM cbt.exam_package WHERE id = $1 AND deleted_at IS NULL`, id).Scan(&examID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, pgx.ErrNoRows
		}
		return nil, err
	}
	if _, err := r.pool.Exec(ctx,
		`UPDATE cbt.exam SET exam_code = $2, title = $3, updated_at = NOW() WHERE id = $1 AND deleted_at IS NULL`,
		examID, req.Code, req.Name); err != nil {
		return nil, err
	}
	tag, err := r.pool.Exec(ctx,
		`UPDATE cbt.exam_package SET exam_id = $2, is_active = COALESCE($3, is_active) WHERE id = $1 AND deleted_at IS NULL`,
		id, examID, req.IsActive)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, pgx.ErrNoRows
	}
	var gradeID *uuid.UUID
	if req.GradeID != nil && *req.GradeID != "" {
		if gid, err := uuid.Parse(*req.GradeID); err == nil {
			gradeID = &gid
		}
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `DELETE FROM cbt.exam_grade WHERE exam_id = $1`, examID); err != nil {
		return nil, err
	}
	if gradeID != nil {
		if _, err := tx.Exec(ctx,
			`INSERT INTO cbt.exam_grade (exam_id, grade_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			examID, *gradeID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return r.GetByID(ctx, id)
}

func (r *Repository) Delete(ctx context.Context, id uuid.UUID) error {
	// Hard delete on the package row only. cbt.exam_package cascades to
	// cbt.exam_package_question; the underlying cbt.exam is left untouched.
	tag, err := r.pool.Exec(ctx, `DELETE FROM cbt.exam_package WHERE id = $1 AND deleted_at IS NULL`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *Repository) ListExams(ctx context.Context, packageID uuid.UUID) ([]PackageExam, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT exam_content_id, subject_id, COALESCE(subject_name, ''), display_order
		 FROM cms.exam_package_exams
		 WHERE package_id = $1
		 ORDER BY display_order`, packageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PackageExam
	for rows.Next() {
		var pe PackageExam
		var subject *uuid.UUID
		if err := rows.Scan(&pe.ExamContentID, &subject, &pe.SubjectName, &pe.DisplayOrder); err != nil {
			return nil, err
		}
		if subject != nil {
			pe.SubjectID = *subject
		}
		out = append(out, pe)
	}
	return out, rows.Err()
}

func (r *Repository) LinkExam(ctx context.Context, packageID uuid.UUID, req LinkExamRequest) error {
	examID, _ := uuid.Parse(req.ExamContentID)
	subjectID, _ := uuid.Parse(req.SubjectID)
	if _, err := r.pool.Exec(ctx,
		`UPDATE cbt.exam_package SET exam_id = $2 WHERE id = $1 AND deleted_at IS NULL`,
		packageID, examID); err != nil {
		return err
	}
	_, err := r.pool.Exec(ctx,
		`INSERT INTO cbt.exam_subject (exam_id, subject_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		examID, subjectID)
	return err
}

func (r *Repository) IsExamContent(ctx context.Context, examContentID uuid.UUID) (bool, error) {
	var ok bool
	err := r.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM cbt.exam WHERE id = $1 AND deleted_at IS NULL)`,
		examContentID).Scan(&ok)
	return ok, err
}

func (r *Repository) UnlinkExam(ctx context.Context, packageID, examContentID uuid.UUID) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM cbt.exam_subject WHERE exam_id = $1`,
		examContentID)
	return err
}

// ---- Service ----

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) List(ctx context.Context, level string) ([]ExamPackage, error) {
	return s.repo.List(ctx, level)
}
func (s *Service) Get(ctx context.Context, id uuid.UUID) (*ExamPackage, error) {
	return s.repo.GetByID(ctx, id)
}
func (s *Service) Create(ctx context.Context, req SavePackageRequest) (*ExamPackage, error) {
	if err := validateSaveRequest(req); err != nil {
		return nil, err
	}
	return s.repo.Create(ctx, req)
}
func (s *Service) Update(ctx context.Context, id uuid.UUID, req SavePackageRequest) (*ExamPackage, error) {
	if err := validateSaveRequest(req); err != nil {
		return nil, err
	}
	return s.repo.Update(ctx, id, req)
}
func (s *Service) Delete(ctx context.Context, id uuid.UUID) error {
	return s.repo.Delete(ctx, id)
}
func (s *Service) ListExams(ctx context.Context, packageID uuid.UUID) ([]PackageExam, error) {
	return s.repo.ListExams(ctx, packageID)
}
func (s *Service) LinkExam(ctx context.Context, packageID uuid.UUID, req LinkExamRequest) error {
	if err := validateLinkRequest(req); err != nil {
		return err
	}
	examID, _ := uuid.Parse(req.ExamContentID)
	ok, err := s.repo.IsExamContent(ctx, examID)
	if err != nil {
		return err
	}
	if !ok {
		return fiber.NewError(fiber.StatusBadRequest, "exam_content_id must reference an exam content")
	}
	return s.repo.LinkExam(ctx, packageID, req)
}
func (s *Service) UnlinkExam(ctx context.Context, packageID, examContentID uuid.UUID) error {
	return s.repo.UnlinkExam(ctx, packageID, examContentID)
}

// ---- Handler ----

type Handler struct {
	svc  *Service
	auth string
}

func NewHandler(svc *Service, secret string) *Handler {
	return &Handler{svc: svc, auth: secret}
}

func (h *Handler) RegisterRoutes(router fiber.Router) {
	authM := middleware.RequireAuth(h.auth)
	write := middleware.RequireRole("SUPER_ADMIN", "STAFF", "GURU")

	r := router.Group("/exam-packages", authM)
	r.Get("/", h.List)
	r.Post("/", write, h.Create)
	r.Get("/:id", h.Get)
	r.Put("/:id", write, h.Update)
	r.Delete("/:id", write, h.Delete)
	r.Get("/:id/exams", h.ListExams)
	r.Post("/:id/exams", write, h.LinkExam)
	r.Delete("/:id/exams/:examContentId", write, h.UnlinkExam)
}

func (h *Handler) parseID(c *fiber.Ctx) (uuid.UUID, error) {
	id, err := uuid.Parse(c.Params("id"))
	if err != nil {
		return uuid.Nil, fiber.NewError(fiber.StatusBadRequest, "Invalid package ID")
	}
	return id, nil
}

func (h *Handler) List(c *fiber.Ctx) error {
	items, err := h.svc.List(c.Context(), c.Query("education_level"))
	if err != nil {
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to list exam packages"))
	}
	if items == nil {
		items = []ExamPackage{}
	}
	return c.JSON(shared.Success(items))
}

func (h *Handler) Get(c *fiber.Ctx) error {
	id, err := h.parseID(c)
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, err.Error()))
	}
	pkg, err := h.svc.Get(c.Context(), id)
	if err != nil {
		return c.Status(404).JSON(shared.Error(shared.ErrNotFound, "Exam package not found"))
	}
	subjects, err := h.svc.ListExams(c.Context(), id)
	if err != nil {
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to list package exams"))
	}
	if subjects == nil {
		subjects = []PackageExam{}
	}
	if len(subjects) == 1 {
		subjects[0].DisplayOrder = 1
	}
	return c.JSON(shared.Success(fiber.Map{
		"id":              pkg.ID,
		"code":            pkg.Code,
		"name":            pkg.Name,
		"education_level": pkg.EducationLevel,
		"grade_id":        pkg.GradeID,
		"is_active":       pkg.IsActive,
		"created_at":      pkg.CreatedAt,
		"updated_at":      pkg.UpdatedAt,
		"subjects_count":  len(subjects),
		"total_questions": pkg.TotalQuestions,
		"duration_minutes": pkg.DurationMinutes,
		"subjects":        subjects,
		"package_mode":    "SINGLE",
	}))
}

func (h *Handler) Create(c *fiber.Ctx) error {
	var req SavePackageRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid request body"))
	}
	p, err := h.svc.Create(c.Context(), req)
	if err != nil {
		if e, ok := err.(*fiber.Error); ok {
			return c.Status(e.Code).JSON(shared.Error(shared.ErrorCode(e.Message), e.Message))
		}
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to create exam package"))
	}
	return c.Status(201).JSON(shared.Success(p))
}

func (h *Handler) Update(c *fiber.Ctx) error {
	id, err := h.parseID(c)
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, err.Error()))
	}
	var req SavePackageRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid request body"))
	}
	p, err := h.svc.Update(c.Context(), id, req)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return c.Status(404).JSON(shared.Error(shared.ErrNotFound, "Exam package not found"))
		}
		if e, ok := err.(*fiber.Error); ok {
			return c.Status(e.Code).JSON(shared.Error(shared.ErrorCode(e.Message), e.Message))
		}
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to update exam package"))
	}
	return c.JSON(shared.Success(p))
}

func (h *Handler) Delete(c *fiber.Ctx) error {
	id, err := h.parseID(c)
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, err.Error()))
	}
	if err := h.svc.Delete(c.Context(), id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return c.Status(404).JSON(shared.Error(shared.ErrNotFound, "Exam package not found"))
		}
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to delete exam package"))
	}
	return c.JSON(shared.Success(map[string]string{"status": "deleted"}))
}

func (h *Handler) ListExams(c *fiber.Ctx) error {
	id, err := h.parseID(c)
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, err.Error()))
	}
	items, err := h.svc.ListExams(c.Context(), id)
	if err != nil {
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to list package exams"))
	}
	if items == nil {
		items = []PackageExam{}
	}
	return c.JSON(shared.Success(items))
}

func (h *Handler) LinkExam(c *fiber.Ctx) error {
	id, err := h.parseID(c)
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, err.Error()))
	}
	var req LinkExamRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid request body"))
	}
	if err := h.svc.LinkExam(c.Context(), id, req); err != nil {
		if e, ok := err.(*fiber.Error); ok {
			return c.Status(e.Code).JSON(shared.Error(shared.ErrorCode(e.Message), e.Message))
		}
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to link exam"))
	}
	exams, err := h.svc.ListExams(c.Context(), id)
	if err != nil {
		exams = []PackageExam{}
	}
	return c.JSON(shared.Success(exams))
}

func (h *Handler) UnlinkExam(c *fiber.Ctx) error {
	id, err := h.parseID(c)
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, err.Error()))
	}
	examID, err := uuid.Parse(c.Params("examContentId"))
	if err != nil {
		return c.Status(400).JSON(shared.Error(shared.ErrValidation, "Invalid exam content ID"))
	}
	if err := h.svc.UnlinkExam(c.Context(), id, examID); err != nil {
		return c.Status(500).JSON(shared.Error(shared.ErrInternal, "Failed to unlink exam"))
	}
	return c.JSON(shared.Success(map[string]string{"status": "unlinked"}))
}
