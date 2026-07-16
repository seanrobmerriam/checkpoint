package journal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // driver registration
)

// Config holds the parameters for opening a journal DB.
//
// The driver enforces single-writer semantics: callers must serialize
// writes themselves if they want to be safe under concurrency. Phase 1
// only asks for correctness on a single goroutine; concurrency is added
// in Phase 4.
type Config struct {
	// Path is the SQLite file path. Use "file::memory:?cache=shared" for
	// an in-memory DB (the cache=shared part allows multiple connections
	// in the same process to see the same data — needed for tests that
	// open two readers).
	Path string

	// RedactArgs, when true, replaces the on-disk `arguments` blob with
	// a stub. The signature hash still derives from the original
	// arguments, so divergence detection continues to work via hash
	// comparison — but human-readable value diffs are no longer
	// possible. See spec §4.
	RedactArgs bool
}

// DB is a journal backed by SQLite (modernc.org/sqlite, WAL,
// synchronous=FULL).
//
// All write methods are required to fsync before returning. SQLite with
// synchronous=FULL performs an fsync at every COMMIT, so the durability
// guarantee is provided by wrapping each write in a transaction and
// committing it.
type DB struct {
	sqlDB *sql.DB
	cfg   Config
}

// Open opens (or creates) a SQLite database, applies the required
// pragmas, and creates the schema if needed.
func Open(cfg Config) (*DB, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("journal: Config.Path is required")
	}
	sqlDB, err := sql.Open("sqlite", cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("journal.Open: %w", err)
	}

	// Limit to single connection: SQLite WAL allows multiple readers
	// but only one writer. A single connection is the simplest way to
	// guarantee no busy errors at this layer; concurrency will be
	// added at the proxy level in Phase 4.
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	d := &DB{sqlDB: sqlDB, cfg: cfg}
	if err := d.applyPragmas(context.Background()); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("journal.Open pragmas: %w", err)
	}
	if err := d.createSchema(context.Background()); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("journal.Open schema: %w", err)
	}
	return d, nil
}

func (d *DB) applyPragmas(ctx context.Context) error {
	// WAL: concurrent readers and single writer; crash-safer than the
	// default rollback journal.
	if _, err := d.sqlDB.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("PRAGMA journal_mode=WAL: %w", err)
	}
	// synchronous=FULL: fsync at every commit. This is what makes the
	// durability guarantee hold. Don't downgrade.
	if _, err := d.sqlDB.ExecContext(ctx, "PRAGMA synchronous=FULL"); err != nil {
		return fmt.Errorf("PRAGMA synchronous=FULL: %w", err)
	}
	// Foreign keys aren't used yet; leave off (default).
	if _, err := d.sqlDB.ExecContext(ctx, "PRAGMA busy_timeout=5000"); err != nil {
		return fmt.Errorf("PRAGMA busy_timeout: %w", err)
	}
	return nil
}

func (d *DB) createSchema(ctx context.Context) error {
	stmt := `
CREATE TABLE IF NOT EXISTS calls (
  workflow_id    TEXT NOT NULL,
  sequence       INTEGER NOT NULL,
  tool_name      TEXT NOT NULL,
  call_signature TEXT NOT NULL,
  arguments      BLOB NOT NULL,
  status         TEXT NOT NULL,
  result         BLOB,
  error          TEXT,
  started_at     INTEGER NOT NULL,
  completed_at   INTEGER,
  PRIMARY KEY (workflow_id, sequence)
);
CREATE INDEX IF NOT EXISTS calls_by_status ON calls(workflow_id, status);
`
	_, err := d.sqlDB.ExecContext(ctx, stmt)
	return err
}

// Conn returns the underlying *sql.DB. The journal package uses this
// for diagnostic queries (e.g. counting PENDINGs); write paths
// should still go through the journal.* methods so the durability
// invariants stay enforced.
func (d *DB) Conn() *sql.DB { return d.sqlDB }

// Close releases the underlying connection pool.
func (d *DB) Close() error {
	if d == nil || d.sqlDB == nil {
		return nil
	}
	return d.sqlDB.Close()
}

// ErrSequenceLeapBack is returned if the journal ever sees a sequence
// number smaller than an earlier-observed one — should be impossible if
// writers are serialized, and is loud if it ever isn't.
var ErrSequenceLeapBack = errors.New("journal: observed a sequence leap backward")

// ErrUncommittedSequence is returned if a workflow already has a row at
// this sequence — i.e. the caller tried to reserve a sequence that was
// taken by a previous (still-uncommitted-or-completed) call.
var ErrUncommittedSequence = errors.New("journal: sequence already in use")

// MaxSequence returns the highest sequence number currently in the
// journal for a given workflow, or 0 if there are none. Used as the
// basis for NextSequence.
//
// This is a read; it does not require a write transaction.
func (d *DB) MaxSequence(ctx context.Context, workflowID string) (int64, error) {
	var maxSeq sql.NullInt64
	err := d.sqlDB.QueryRowContext(ctx,
		"SELECT MAX(sequence) FROM calls WHERE workflow_id = ?", workflowID,
	).Scan(&maxSeq)
	if err != nil {
		return 0, fmt.Errorf("journal.MaxSequence: %w", err)
	}
	if !maxSeq.Valid {
		return 0, nil
	}
	return maxSeq.Int64, nil
}

// BeginRequest returns the next sequence number for this workflow AND
// the existing entry at that sequence (or nil if absent).
//
// incomingSig is unused by the current algorithm but kept on the
// signature for forward compatibility.
//
// targetSeq lets callers pin the lookup to a specific sequence. The
// default value (0) means "use MAX(sequence)+1": a fresh slot for
// forward progress, with `existing == nil`. Setting a non-zero
// targetSeq routes the lookup to that exact sequence, which is how
// the divergence-detection path (BeginRequestAt in §3.4) surfaces
// existing COMPLETED/FAILED entries for replay/divergence checks.
// See also BeginRequestAt for an explicit form of that.
//
// Selection rules:
//
//  1. IN_DOUBT re-encounter. If any row at any sequence has status
//     PENDING, return the lowest such sequence with the PENDING entry.
//     §3.4 demands the proxy re-surface the IN_DOUBT case on every
//     subsequent call until reconciled, never skip past it.
//  2. Pinned target. If targetSeq > 0, return (targetSeq, entry|nil
//     at that exact sequence). Never surfaces PENDING — use a
//     separate path for that, since IN_DOUBT and replay-detection
//     are orthogonal decisions.
//  3. Forward progress. Otherwise (targetSeq == 0, no PENDING)
//     return MAX(sequence) + 1 with nil — a fresh slot.
//
// Spec note: §3.2 explicitly forbids content-hash-only idempotency
// keys ("two legitimate, distinct calls to the same tool with the
// same arguments are common and must not collide"). Each call is its
// own sequence slot, even if the call signature collides with one
// observed earlier in the workflow. Forward progress is monotonic
// forever; we never collapse calls with matching signatures.
func (d *DB) BeginRequest(ctx context.Context, workflowID, incomingSig string, targetSeq int64) (seq int64, existing *Entry, err error) {
	_ = incomingSig
	tx, err := d.sqlDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, nil, fmt.Errorf("journal.BeginRequest: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// (1) PENDING first — never skip past an IN_DOUBT.
	row := tx.QueryRowContext(ctx, `
SELECT sequence, status, call_signature, result, error
FROM calls
WHERE workflow_id = ? AND status = ?
ORDER BY sequence ASC
LIMIT 1
`, workflowID, string(StatusPending))
	var (
		pendSeq   int64
		pStatus   string
		pSig      string
		pRes      []byte
		pErrStr   sql.NullString
	)
	switch err := row.Scan(&pendSeq, &pStatus, &pSig, &pRes, &pErrStr); {
	case err == nil:
		return pendSeq, &Entry{
			Status:        Status(pStatus),
			CallSignature: pSig,
			Result:        pRes,
			Error:         pErrStr.String,
		}, nil
	case errors.Is(err, sql.ErrNoRows):
		// fall through to (2)
	default:
		return 0, nil, fmt.Errorf("journal.BeginRequest: select pending: %w", err)
	}

	// (2) Pinned target. Look up the entry at exactly targetSeq.
	if targetSeq > 0 {
		var (
			st     string
			sig    string
			resBuf []byte
			errStr sql.NullString
		)
		row := tx.QueryRowContext(ctx, `
SELECT status, call_signature, result, error
FROM calls
WHERE workflow_id = ? AND sequence = ?
`, workflowID, targetSeq)
		switch err := row.Scan(&st, &sig, &resBuf, &errStr); {
		case errors.Is(err, sql.ErrNoRows):
			return targetSeq, nil, nil
		case err != nil:
			return 0, nil, fmt.Errorf("journal.BeginRequest: target lookup: %w", err)
		}
		return targetSeq, &Entry{
			Status:        Status(st),
			CallSignature: sig,
			Result:        resBuf,
			Error:         errStr.String,
		}, nil
	}

	// (3) Forward progress — fresh slot.
	var maxSeq sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		"SELECT MAX(sequence) FROM calls WHERE workflow_id = ?", workflowID,
	).Scan(&maxSeq); err != nil {
		return 0, nil, fmt.Errorf("journal.BeginRequest: max: %w", err)
	}
	maxVal := int64(0)
	if maxSeq.Valid {
		maxVal = maxSeq.Int64
	}
	return maxVal + 1, nil, nil
}

// ReservePending writes a PENDING row at the given sequence. This is the
// fsync point for "intent to call" — the call to upstream must not
// begin until this method has returned successfully.
//
// The row's arguments are the raw JSON of the call. The signature is the
// hash already computed by the idempotency package. started_at is set
// to the wall-clock time at reservation; the only property we rely on
// is monotonicity between PENDING and the matching COMPLETED/FAILED.
//
// A nil/empty args is stored as the JSON literal "null" so that the
// NOT NULL column constraint is satisfied and the storage always
// contains valid JSON. (Empty arguments are semantically distinct from
// "null" arguments; callers that care should marshal "null" themselves
// before calling.)
//
// When cfg.RedactArgs is true (set via SetRedactArgs), `args` is
// replaced with a stub `{"_redacted":true}` block — only the on-disk
// call_signature is retained for replay-detection purposes. The
// spec notes that this breaks divergence-detection's ability to show
// a human-readable value diff; see the README.
func (d *DB) ReservePending(ctx context.Context, workflowID string, seq int64, tool, signature string, args []byte) error {
	if len(args) == 0 {
		args = []byte("null")
	}
	if d.cfg.RedactArgs {
		// Replace raw args with a marker — the signature still hashes
		// the *original* args (it was computed before this point), so
		// divergence detection can still compare hashes against the
		// replayed call.
		args = []byte(`{"_redacted":true}`)
	}
	tx, err := d.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("journal.ReservePending: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `
INSERT INTO calls (workflow_id, sequence, tool_name, call_signature, arguments, status, started_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
`, workflowID, seq, tool, signature, args, string(StatusPending), time.Now().UnixNano()); err != nil {
		return fmt.Errorf("journal.ReservePending: insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("journal.ReservePending: commit: %w", err)
	}
	return nil
}

// Complete updates a PENDING row to COMPLETED with the stored result.
// MUST fsync via the commit before returning. result is the raw JSON of
// the upstream tool result; nil/empty is allowed.
func (d *DB) Complete(ctx context.Context, workflowID string, seq int64, result []byte) error {
	return d.finalize(ctx, workflowID, seq, string(StatusCompleted), result, "")
}

// Fail updates a PENDING row to FAILED with the stored error.
// MUST fsync via the commit before returning.
func (d *DB) Fail(ctx context.Context, workflowID string, seq int64, errMsg string) error {
	return d.finalize(ctx, workflowID, seq, string(StatusFailed), nil, errMsg)
}

func (d *DB) finalize(ctx context.Context, workflowID string, seq int64, status string, result []byte, errMsg string) error {
	tx, err := d.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("journal.%s: begin: %w", status, err)
	}
	defer func() { _ = tx.Rollback() }()

	// Verify the row is currently PENDING. We refuse to silently flip a
	// row out of PENDING — that should be impossible given correct
	// single-writer usage, but we want loud failures not silent
	// corruption if it ever happens.
	var currentStatus string
	if err := tx.QueryRowContext(ctx,
		"SELECT status FROM calls WHERE workflow_id = ? AND sequence = ?",
		workflowID, seq,
	).Scan(&currentStatus); err != nil {
		return fmt.Errorf("journal.%s: select: %w", status, err)
	}
	if currentStatus != string(StatusPending) {
		return fmt.Errorf("journal.%s: row at (%s, %d) has status %q, want PENDING",
			status, workflowID, seq, currentStatus)
	}

	if _, err := tx.ExecContext(ctx, `
UPDATE calls
SET status = ?, result = ?, error = ?, completed_at = ?
WHERE workflow_id = ? AND sequence = ?
`, status, result, errMsg, time.Now().UnixNano(), workflowID, seq); err != nil {
		return fmt.Errorf("journal.%s: update: %w", status, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("journal.%s: commit: %w", status, err)
	}
	return nil
}

// Lookup directly fetches a sequence's row — used by tests and by
// future tools that want to inspect a specific journal entry without
// running BeginRequest.
func (d *DB) Lookup(ctx context.Context, workflowID string, seq int64) (*Entry, error) {
	var (
		st     string
		sig    string
		resBuf []byte
		errStr sql.NullString
	)
	row := d.sqlDB.QueryRowContext(ctx,
		`SELECT status, call_signature, result, error FROM calls WHERE workflow_id = ? AND sequence = ?`,
		workflowID, seq,
	)
	switch err := row.Scan(&st, &sig, &resBuf, &errStr); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("journal.Lookup: %w", err)
	}
	return &Entry{
		Status:        Status(st),
		CallSignature: sig,
		Result:        resBuf,
		Error:         errStr.String,
	}, nil
}
