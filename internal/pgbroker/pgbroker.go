// Package pgbroker implements the base.Broker interface using PostgreSQL
// via the standard database/sql package.
package pgbroker

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/timeutil"
)

const leaseDuration = 30 * time.Second

const (
	maxArchiveSize           = 10000
	archivedExpirationInDays = 90
	statsTTL                 = 90 * 24 * time.Hour
)

// PGBroker implements base.Broker backed by PostgreSQL.
type PGBroker struct {
	db    *sql.DB
	clock timeutil.Clock
	mu    sync.Mutex
	done  chan struct{}
}

// New returns a new PGBroker. It runs schema migrations on first call.
func New(db *sql.DB) (*PGBroker, error) {
	if _, err := db.Exec(createSchemaSQL); err != nil {
		return nil, fmt.Errorf("pgbroker: schema migration failed: %w", err)
	}
	return &PGBroker{
		db:    db,
		clock: timeutil.NewRealClock(),
		done:  make(chan struct{}),
	}, nil
}

func (b *PGBroker) SetClock(c timeutil.Clock) { b.clock = c }

func (b *PGBroker) Ping() error {
	return b.db.Ping()
}

func (b *PGBroker) Close() error {
	return b.db.Close()
}

func (b *PGBroker) ensureQueue(ctx context.Context, tx *sql.Tx, qname string) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO asynq_queues (qname) VALUES ($1) ON CONFLICT DO NOTHING`, qname)
	return err
}

func (b *PGBroker) Enqueue(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "pgbroker.Enqueue"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	if err := b.ensureQueue(ctx, tx, msg.Queue); err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("ensure queue: %v", err))
	}

	var exists bool
	err = tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM asynq_tasks WHERE queue=$1 AND id=$2)`,
		msg.Queue, msg.ID).Scan(&exists)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("check exists: %v", err))
	}
	if exists {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}

	now := b.clock.Now().UnixNano()
	_, err = tx.ExecContext(ctx,
		`INSERT INTO asynq_tasks (queue, id, msg, state, pending_since)
		 VALUES ($1, $2, $3, 'pending', $4)`,
		msg.Queue, msg.ID, encoded, now)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("insert task: %v", err))
	}
	return tx.Commit()
}

func (b *PGBroker) EnqueueUnique(ctx context.Context, msg *base.TaskMessage, ttl time.Duration) error {
	var op errors.Op = "pgbroker.EnqueueUnique"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	if err := b.ensureQueue(ctx, tx, msg.Queue); err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("ensure queue: %v", err))
	}

	// Clean expired unique locks.
	now := b.clock.Now()
	_, _ = tx.ExecContext(ctx,
		`DELETE FROM asynq_unique_locks WHERE expire_at <= $1`, now.Unix())

	// Try to acquire uniqueness lock.
	var lockExists bool
	err = tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM asynq_unique_locks WHERE key=$1)`,
		msg.UniqueKey).Scan(&lockExists)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("check unique lock: %v", err))
	}
	if lockExists {
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}

	var taskExists bool
	err = tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM asynq_tasks WHERE queue=$1 AND id=$2)`,
		msg.Queue, msg.ID).Scan(&taskExists)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("check task exists: %v", err))
	}
	if taskExists {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO asynq_unique_locks (key, task_id, expire_at) VALUES ($1, $2, $3)`,
		msg.UniqueKey, msg.ID, now.Add(ttl).Unix())
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("insert unique lock: %v", err))
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO asynq_tasks (queue, id, msg, state, unique_key, pending_since)
		 VALUES ($1, $2, $3, 'pending', $4, $5)`,
		msg.Queue, msg.ID, encoded, msg.UniqueKey, now.UnixNano())
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("insert task: %v", err))
	}
	return tx.Commit()
}

func (b *PGBroker) BatchEnqueue(ctx context.Context, items []base.BatchEnqueueItem) (int, error) {
	var op errors.Op = "pgbroker.BatchEnqueue"
	if len(items) == 0 {
		return 0, nil
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	now := b.clock.Now()
	enqueued := 0
	for _, item := range items {
		encoded, err := base.EncodeMessage(item.Msg)
		if err != nil {
			return 0, errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
		}
		_ = b.ensureQueue(ctx, tx, item.Msg.Queue)

		state := "pending"
		score := int64(0)
		pendingSince := now.UnixNano()
		if !item.ProcessAt.IsZero() {
			state = "scheduled"
			score = item.ProcessAt.Unix()
			pendingSince = 0
		}

		res, err := tx.ExecContext(ctx,
			`INSERT INTO asynq_tasks (queue, id, msg, state, score, pending_since)
			 VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING`,
			item.Msg.Queue, item.Msg.ID, encoded, state, score, pendingSince)
		if err != nil {
			continue
		}
		rows, _ := res.RowsAffected()
		enqueued += int(rows)
	}
	if err := tx.Commit(); err != nil {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("commit: %v", err))
	}
	return enqueued, nil
}

func (b *PGBroker) Dequeue(qnames ...string) (*base.TaskMessage, time.Time, error) {
	var op errors.Op = "pgbroker.Dequeue"
	now := b.clock.Now()
	leaseExpire := now.Add(leaseDuration)

	for _, qname := range qnames {
		// Check if queue is paused.
		var paused bool
		err := b.db.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM asynq_tasks WHERE queue=$1 AND id='__paused__' AND state='paused')`,
			qname).Scan(&paused)
		if err == nil && paused {
			continue
		}

		// Dequeue: pick one pending task (FIFO by pending_since) and move to active.
		tx, err := b.db.Begin()
		if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
		}

		var encoded []byte
		var taskID string
		err = tx.QueryRow(
			`SELECT id, msg FROM asynq_tasks
			 WHERE queue=$1 AND state='pending'
			 ORDER BY pending_since ASC
			 LIMIT 1
			 FOR UPDATE SKIP LOCKED`,
			qname).Scan(&taskID, &encoded)
		if err == sql.ErrNoRows {
			tx.Rollback()
			continue
		}
		if err != nil {
			tx.Rollback()
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("select task: %v", err))
		}

		_, err = tx.Exec(
			`UPDATE asynq_tasks SET state='active', pending_since=0, lease_expire=$1
			 WHERE queue=$2 AND id=$3`,
			leaseExpire.Unix(), qname, taskID)
		if err != nil {
			tx.Rollback()
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("update task: %v", err))
		}

		if err := tx.Commit(); err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("commit: %v", err))
		}

		msg, err := base.DecodeMessage(encoded)
		if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("decode message: %v", err))
		}
		return msg, leaseExpire, nil
	}
	return nil, time.Time{}, errors.E(op, errors.NotFound, errors.ErrNoProcessableTask)
}

func (b *PGBroker) Done(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "pgbroker.Done"
	now := b.clock.Now()

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx,
		`DELETE FROM asynq_tasks WHERE queue=$1 AND id=$2 AND state='active'`,
		msg.Queue, msg.ID)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("delete task: %v", err))
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errors.E(op, errors.NotFound, "task not found in active state")
	}

	if len(msg.UniqueKey) > 0 {
		_, _ = tx.ExecContext(ctx,
			`DELETE FROM asynq_unique_locks WHERE key=$1 AND task_id=$2`,
			msg.UniqueKey, msg.ID)
	}

	b.incrStats(ctx, tx, msg.Queue, now, 1, 0)
	return tx.Commit()
}

func (b *PGBroker) MarkAsComplete(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "pgbroker.MarkAsComplete"
	now := b.clock.Now()
	msg.CompletedAt = now.Unix()
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	expireAt := now.Unix() + msg.Retention

	res, err := tx.ExecContext(ctx,
		`UPDATE asynq_tasks SET state='completed', msg=$1, score=$2, completed_at=$3, lease_expire=0
		 WHERE queue=$4 AND id=$5 AND state='active'`,
		encoded, expireAt, now.Unix(), msg.Queue, msg.ID)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("update task: %v", err))
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errors.E(op, errors.NotFound, "task not found in active state")
	}

	if len(msg.UniqueKey) > 0 {
		_, _ = tx.ExecContext(ctx,
			`DELETE FROM asynq_unique_locks WHERE key=$1 AND task_id=$2`,
			msg.UniqueKey, msg.ID)
	}

	b.incrStats(ctx, tx, msg.Queue, now, 1, 0)
	return tx.Commit()
}

func (b *PGBroker) Requeue(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "pgbroker.Requeue"
	res, err := b.db.ExecContext(ctx,
		`UPDATE asynq_tasks SET state='pending', pending_since=$1, lease_expire=0
		 WHERE queue=$2 AND id=$3 AND state='active'`,
		b.clock.Now().UnixNano(), msg.Queue, msg.ID)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("update task: %v", err))
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errors.E(op, errors.NotFound, "task not found in active state")
	}
	return nil
}

func (b *PGBroker) Schedule(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	var op errors.Op = "pgbroker.Schedule"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	if err := b.ensureQueue(ctx, tx, msg.Queue); err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("ensure queue: %v", err))
	}

	var exists bool
	err = tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM asynq_tasks WHERE queue=$1 AND id=$2)`,
		msg.Queue, msg.ID).Scan(&exists)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("check exists: %v", err))
	}
	if exists {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO asynq_tasks (queue, id, msg, state, score)
		 VALUES ($1, $2, $3, 'scheduled', $4)`,
		msg.Queue, msg.ID, encoded, processAt.Unix())
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("insert task: %v", err))
	}
	return tx.Commit()
}

func (b *PGBroker) ScheduleUnique(ctx context.Context, msg *base.TaskMessage, processAt time.Time, ttl time.Duration) error {
	var op errors.Op = "pgbroker.ScheduleUnique"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	if err := b.ensureQueue(ctx, tx, msg.Queue); err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("ensure queue: %v", err))
	}

	now := b.clock.Now()
	_, _ = tx.ExecContext(ctx,
		`DELETE FROM asynq_unique_locks WHERE expire_at <= $1`, now.Unix())

	var lockExists bool
	err = tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM asynq_unique_locks WHERE key=$1)`,
		msg.UniqueKey).Scan(&lockExists)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("check unique lock: %v", err))
	}
	if lockExists {
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}

	var taskExists bool
	err = tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM asynq_tasks WHERE queue=$1 AND id=$2)`,
		msg.Queue, msg.ID).Scan(&taskExists)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("check task exists: %v", err))
	}
	if taskExists {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO asynq_unique_locks (key, task_id, expire_at) VALUES ($1, $2, $3)`,
		msg.UniqueKey, msg.ID, now.Add(ttl).Unix())
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("insert unique lock: %v", err))
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO asynq_tasks (queue, id, msg, state, unique_key, score)
		 VALUES ($1, $2, $3, 'scheduled', $4, $5)`,
		msg.Queue, msg.ID, encoded, msg.UniqueKey, processAt.Unix())
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("insert task: %v", err))
	}
	return tx.Commit()
}

func (b *PGBroker) Retry(ctx context.Context, msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	var op errors.Op = "pgbroker.Retry"
	now := b.clock.Now()
	modified := *msg
	if isFailure {
		modified.Retried++
	}
	modified.ErrorMsg = errMsg
	modified.LastFailedAt = now.Unix()
	encoded, err := base.EncodeMessage(&modified)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx,
		`UPDATE asynq_tasks SET state='retry', msg=$1, score=$2, lease_expire=0
		 WHERE queue=$3 AND id=$4 AND state='active'`,
		encoded, processAt.Unix(), msg.Queue, msg.ID)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("update task: %v", err))
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errors.E(op, errors.NotFound, "task not found in active state")
	}

	if isFailure {
		b.incrStats(ctx, tx, msg.Queue, now, 1, 1)
	}
	return tx.Commit()
}

func (b *PGBroker) Archive(ctx context.Context, msg *base.TaskMessage, errMsg string) error {
	var op errors.Op = "pgbroker.Archive"
	now := b.clock.Now()
	modified := *msg
	modified.ErrorMsg = errMsg
	modified.LastFailedAt = now.Unix()
	encoded, err := base.EncodeMessage(&modified)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx,
		`UPDATE asynq_tasks SET state='archived', msg=$1, score=$2, lease_expire=0
		 WHERE queue=$3 AND id=$4 AND state='active'`,
		encoded, now.Unix(), msg.Queue, msg.ID)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("update task: %v", err))
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errors.E(op, errors.NotFound, "task not found in active state")
	}

	cutoff := now.AddDate(0, 0, -archivedExpirationInDays).Unix()
	_, _ = tx.ExecContext(ctx,
		`DELETE FROM asynq_tasks WHERE queue=$1 AND state='archived' AND score < $2`,
		msg.Queue, cutoff)

	// Trim archive to maxArchiveSize by removing oldest entries.
	_, _ = tx.ExecContext(ctx,
		`DELETE FROM asynq_tasks WHERE queue=$1 AND state='archived' AND id IN (
		   SELECT id FROM asynq_tasks WHERE queue=$1 AND state='archived'
		   ORDER BY score ASC
		   OFFSET $2
		 )`, msg.Queue, maxArchiveSize)

	b.incrStats(ctx, tx, msg.Queue, now, 1, 1)
	return tx.Commit()
}

func (b *PGBroker) ForwardIfReady(qnames ...string) error {
	var op errors.Op = "pgbroker.ForwardIfReady"
	now := b.clock.Now()

	for _, qname := range qnames {
		for {
			tx, err := b.db.Begin()
			if err != nil {
				return errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
			}

			// Move up to 100 tasks from scheduled/retry that are due.
			rows, err := tx.Query(
				`SELECT id, group_key FROM asynq_tasks
				 WHERE queue=$1 AND state IN ('scheduled', 'retry') AND score <= $2
				 ORDER BY score ASC LIMIT 100
				 FOR UPDATE SKIP LOCKED`,
				qname, now.Unix())
			if err != nil {
				tx.Rollback()
				return errors.E(op, errors.Internal, fmt.Sprintf("query: %v", err))
			}

			type readyTask struct {
				id       string
				groupKey string
			}
			var tasks []readyTask
			for rows.Next() {
				var t readyTask
				if err := rows.Scan(&t.id, &t.groupKey); err != nil {
					rows.Close()
					tx.Rollback()
					return errors.E(op, errors.Internal, fmt.Sprintf("scan: %v", err))
				}
				tasks = append(tasks, t)
			}
			rows.Close()

			if len(tasks) == 0 {
				tx.Rollback()
				break
			}

			for _, t := range tasks {
				if t.groupKey != "" {
					_, err = tx.Exec(
						`UPDATE asynq_tasks SET state='aggregating', score=$1
						 WHERE queue=$2 AND id=$3`,
						now.Unix(), qname, t.id)
				} else {
					_, err = tx.Exec(
						`UPDATE asynq_tasks SET state='pending', pending_since=$1, score=0
						 WHERE queue=$2 AND id=$3`,
						now.UnixNano(), qname, t.id)
				}
				if err != nil {
					tx.Rollback()
					return errors.E(op, errors.Internal, fmt.Sprintf("update: %v", err))
				}
			}
			if err := tx.Commit(); err != nil {
				return errors.E(op, errors.Internal, fmt.Sprintf("commit: %v", err))
			}

			if len(tasks) < 100 {
				break
			}
		}
	}
	return nil
}

func (b *PGBroker) AddToGroup(ctx context.Context, msg *base.TaskMessage, gname string) error {
	var op errors.Op = "pgbroker.AddToGroup"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	if err := b.ensureQueue(ctx, tx, msg.Queue); err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("ensure queue: %v", err))
	}

	var exists bool
	err = tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM asynq_tasks WHERE queue=$1 AND id=$2)`,
		msg.Queue, msg.ID).Scan(&exists)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("check exists: %v", err))
	}
	if exists {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO asynq_tasks (queue, id, msg, state, group_key, score)
		 VALUES ($1, $2, $3, 'aggregating', $4, $5)`,
		msg.Queue, msg.ID, encoded, gname, b.clock.Now().Unix())
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("insert task: %v", err))
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO asynq_groups (queue, gname) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		msg.Queue, gname)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("insert group: %v", err))
	}
	return tx.Commit()
}

func (b *PGBroker) AddToGroupUnique(ctx context.Context, msg *base.TaskMessage, groupKey string, ttl time.Duration) error {
	var op errors.Op = "pgbroker.AddToGroupUnique"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	if err := b.ensureQueue(ctx, tx, msg.Queue); err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("ensure queue: %v", err))
	}

	now := b.clock.Now()
	_, _ = tx.ExecContext(ctx,
		`DELETE FROM asynq_unique_locks WHERE expire_at <= $1`, now.Unix())

	uniqueKey := base.UniqueKey(msg.Queue, msg.Type, msg.Payload)

	var lockExists bool
	err = tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM asynq_unique_locks WHERE key=$1)`,
		uniqueKey).Scan(&lockExists)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("check unique lock: %v", err))
	}
	if lockExists {
		return errors.E(op, errors.AlreadyExists, errors.ErrDuplicateTask)
	}

	var taskExists bool
	err = tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM asynq_tasks WHERE queue=$1 AND id=$2)`,
		msg.Queue, msg.ID).Scan(&taskExists)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("check task exists: %v", err))
	}
	if taskExists {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO asynq_unique_locks (key, task_id, expire_at) VALUES ($1, $2, $3)`,
		uniqueKey, msg.ID, now.Add(ttl).Unix())
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("insert unique lock: %v", err))
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO asynq_tasks (queue, id, msg, state, group_key, score)
		 VALUES ($1, $2, $3, 'aggregating', $4, $5)`,
		msg.Queue, msg.ID, encoded, groupKey, now.Unix())
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("insert task: %v", err))
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO asynq_groups (queue, gname) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		msg.Queue, groupKey)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("insert group: %v", err))
	}
	return tx.Commit()
}

func (b *PGBroker) ListGroups(qname string) ([]string, error) {
	var op errors.Op = "pgbroker.ListGroups"
	rows, err := b.db.Query(
		`SELECT gname FROM asynq_groups WHERE queue=$1`, qname)
	if err != nil {
		return nil, errors.E(op, errors.Unknown, fmt.Sprintf("query: %v", err))
	}
	defer rows.Close()

	var groups []string
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("scan: %v", err))
		}
		groups = append(groups, g)
	}
	return groups, nil
}

const aggregationTimeout = 2 * time.Minute

func (b *PGBroker) AggregationCheck(qname, gname string, t time.Time, gracePeriod, maxDelay time.Duration, maxSize int) (string, error) {
	var op errors.Op = "pgbroker.AggregationCheck"

	// Count tasks in group.
	var size int
	err := b.db.QueryRow(
		`SELECT COUNT(*) FROM asynq_tasks WHERE queue=$1 AND state='aggregating' AND group_key=$2`,
		qname, gname).Scan(&size)
	if err != nil {
		return "", errors.E(op, errors.Internal, fmt.Sprintf("count: %v", err))
	}
	if size == 0 {
		return "", nil
	}

	shouldAggregate := false

	// Check max size.
	if maxSize > 0 && size >= maxSize {
		shouldAggregate = true
	}

	if !shouldAggregate && maxDelay > 0 {
		var oldestScore int64
		err := b.db.QueryRow(
			`SELECT COALESCE(MIN(score), 0) FROM asynq_tasks WHERE queue=$1 AND state='aggregating' AND group_key=$2`,
			qname, gname).Scan(&oldestScore)
		if err == nil && oldestScore > 0 && oldestScore <= t.Unix()-int64(maxDelay.Seconds()) {
			shouldAggregate = true
		}
	}

	if !shouldAggregate {
		var latestScore int64
		err := b.db.QueryRow(
			`SELECT COALESCE(MAX(score), 0) FROM asynq_tasks WHERE queue=$1 AND state='aggregating' AND group_key=$2`,
			qname, gname).Scan(&latestScore)
		if err == nil && latestScore > 0 && latestScore <= t.Unix()-int64(gracePeriod.Seconds()) {
			shouldAggregate = true
		}
	}

	if !shouldAggregate {
		return "", nil
	}

	setID := uuid.NewString()
	expireTime := b.clock.Now().Add(aggregationTimeout)

	tx, err := b.db.Begin()
	if err != nil {
		return "", errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	limit := size
	if maxSize > 0 && maxSize < limit {
		limit = maxSize
	}

	rows, err := tx.Query(
		`SELECT id, msg, score FROM asynq_tasks
		 WHERE queue=$1 AND state='aggregating' AND group_key=$2
		 ORDER BY score ASC LIMIT $3
		 FOR UPDATE SKIP LOCKED`,
		qname, gname, limit)
	if err != nil {
		return "", errors.E(op, errors.Internal, fmt.Sprintf("query: %v", err))
	}

	var ids []string
	for rows.Next() {
		var id string
		var encoded []byte
		var score int64
		if err := rows.Scan(&id, &encoded, &score); err != nil {
			rows.Close()
			return "", errors.E(op, errors.Internal, fmt.Sprintf("scan: %v", err))
		}
		_, err := tx.Exec(
			`INSERT INTO asynq_aggregation_sets (queue, gname, set_id, task_id, msg, score, expire_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			qname, gname, setID, id, encoded, score, expireTime.Unix())
		if err != nil {
			rows.Close()
			return "", errors.E(op, errors.Internal, fmt.Sprintf("insert agg set: %v", err))
		}
		ids = append(ids, id)
	}
	rows.Close()

	if len(ids) == 0 {
		return "", nil
	}

	for _, id := range ids {
		_, err := tx.Exec(
			`DELETE FROM asynq_tasks WHERE queue=$1 AND id=$2 AND state='aggregating'`,
			qname, id)
		if err != nil {
			return "", errors.E(op, errors.Internal, fmt.Sprintf("delete from group: %v", err))
		}
	}

	// If group is now empty, remove from groups list.
	var remaining int
	_ = tx.QueryRow(
		`SELECT COUNT(*) FROM asynq_tasks WHERE queue=$1 AND state='aggregating' AND group_key=$2`,
		qname, gname).Scan(&remaining)
	if remaining == 0 {
		_, _ = tx.Exec(`DELETE FROM asynq_groups WHERE queue=$1 AND gname=$2`, qname, gname)
	}

	if err := tx.Commit(); err != nil {
		return "", errors.E(op, errors.Internal, fmt.Sprintf("commit: %v", err))
	}
	return setID, nil
}

func (b *PGBroker) ReadAggregationSet(qname, gname, setID string) ([]*base.TaskMessage, time.Time, error) {
	var op errors.Op = "pgbroker.ReadAggregationSet"

	rows, err := b.db.Query(
		`SELECT msg, expire_at FROM asynq_aggregation_sets
		 WHERE queue=$1 AND gname=$2 AND set_id=$3`,
		qname, gname, setID)
	if err != nil {
		return nil, time.Time{}, errors.E(op, errors.Unknown, fmt.Sprintf("query: %v", err))
	}
	defer rows.Close()

	var msgs []*base.TaskMessage
	var maxExpire int64
	for rows.Next() {
		var encoded []byte
		var expireAt int64
		if err := rows.Scan(&encoded, &expireAt); err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("scan: %v", err))
		}
		msg, err := base.DecodeMessage(encoded)
		if err != nil {
			return nil, time.Time{}, errors.E(op, errors.Internal, fmt.Sprintf("decode: %v", err))
		}
		msgs = append(msgs, msg)
		if expireAt > maxExpire {
			maxExpire = expireAt
		}
	}
	return msgs, time.Unix(maxExpire, 0), nil
}

func (b *PGBroker) DeleteAggregationSet(ctx context.Context, qname, gname, setID string) error {
	var op errors.Op = "pgbroker.DeleteAggregationSet"
	_, err := b.db.ExecContext(ctx,
		`DELETE FROM asynq_aggregation_sets WHERE queue=$1 AND gname=$2 AND set_id=$3`,
		qname, gname, setID)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("delete agg set: %v", err))
	}
	return nil
}

func (b *PGBroker) ReclaimStaleAggregationSets(qname string) error {
	var op errors.Op = "pgbroker.ReclaimStaleAggregationSets"
	now := b.clock.Now().Unix()

	tx, err := b.db.Begin()
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	// Find stale sets.
	rows, err := tx.Query(
		`SELECT DISTINCT set_id, gname FROM asynq_aggregation_sets
		 WHERE queue=$1 AND expire_at <= $2`, qname, now)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("query: %v", err))
	}

	type staleSet struct {
		setID string
		gname string
	}
	var sets []staleSet
	for rows.Next() {
		var s staleSet
		if err := rows.Scan(&s.setID, &s.gname); err != nil {
			rows.Close()
			return errors.E(op, errors.Internal, fmt.Sprintf("scan: %v", err))
		}
		sets = append(sets, s)
	}
	rows.Close()

	for _, s := range sets {
		aggRows, err := tx.Query(
			`SELECT task_id, msg, score FROM asynq_aggregation_sets
			 WHERE queue=$1 AND gname=$2 AND set_id=$3`, qname, s.gname, s.setID)
		if err != nil {
			return errors.E(op, errors.Internal, fmt.Sprintf("query agg: %v", err))
		}
		for aggRows.Next() {
			var taskID string
			var encoded []byte
			var score int64
			if err := aggRows.Scan(&taskID, &encoded, &score); err != nil {
				aggRows.Close()
				return errors.E(op, errors.Internal, fmt.Sprintf("scan agg: %v", err))
			}
			_, _ = tx.Exec(
				`INSERT INTO asynq_tasks (queue, id, msg, state, group_key, score)
				 VALUES ($1, $2, $3, 'aggregating', $4, $5)
				 ON CONFLICT (queue, id) DO UPDATE SET state='aggregating', score=$5`,
				qname, taskID, encoded, s.gname, score)
		}
		aggRows.Close()

		_, _ = tx.Exec(
			`DELETE FROM asynq_aggregation_sets WHERE queue=$1 AND gname=$2 AND set_id=$3`,
			qname, s.gname, s.setID)

		_, _ = tx.Exec(
			`INSERT INTO asynq_groups (queue, gname) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			qname, s.gname)
	}
	return tx.Commit()
}

func (b *PGBroker) DeleteExpiredCompletedTasks(qname string, batchSize int) error {
	now := b.clock.Now().Unix()
	for {
		res, err := b.db.Exec(
			`DELETE FROM asynq_tasks WHERE queue=$1 AND state='completed' AND score <= $2
			 AND id IN (
			   SELECT id FROM asynq_tasks WHERE queue=$1 AND state='completed' AND score <= $2
			   LIMIT $3
			 )`, qname, now, batchSize)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return nil
		}
	}
}

func (b *PGBroker) ListLeaseExpired(cutoff time.Time, qnames ...string) ([]*base.TaskMessage, error) {
	var op errors.Op = "pgbroker.ListLeaseExpired"
	var msgs []*base.TaskMessage
	for _, qname := range qnames {
		rows, err := b.db.Query(
			`SELECT msg FROM asynq_tasks
			 WHERE queue=$1 AND state='active' AND lease_expire <= $2`,
			qname, cutoff.Unix())
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("query: %v", err))
		}
		for rows.Next() {
			var encoded []byte
			if err := rows.Scan(&encoded); err != nil {
				rows.Close()
				return nil, errors.E(op, errors.Internal, fmt.Sprintf("scan: %v", err))
			}
			msg, err := base.DecodeMessage(encoded)
			if err != nil {
				rows.Close()
				return nil, errors.E(op, errors.Internal, fmt.Sprintf("decode: %v", err))
			}
			msgs = append(msgs, msg)
		}
		rows.Close()
	}
	return msgs, nil
}

func (b *PGBroker) ExtendLease(qname string, ids ...string) (time.Time, error) {
	expireAt := b.clock.Now().Add(leaseDuration)
	for _, id := range ids {
		_, _ = b.db.Exec(
			`UPDATE asynq_tasks SET lease_expire=$1
			 WHERE queue=$2 AND id=$3 AND state='active'`,
			expireAt.Unix(), qname, id)
	}
	return expireAt, nil
}

func (b *PGBroker) WriteServerState(info *base.ServerInfo, workers []*base.WorkerInfo, ttl time.Duration) error {
	var op errors.Op = "pgbroker.WriteServerState"
	skey := base.ServerInfoKey(info.Host, info.PID, info.ServerID)
	expireAt := b.clock.Now().Add(ttl).Unix()

	serverData, err := base.EncodeServerInfo(info)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("encode server info: %v", err))
	}

	tx, err := b.db.Begin()
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	_, err = tx.Exec(
		`INSERT INTO asynq_servers (key, data, expire_at) VALUES ($1, $2, $3)
		 ON CONFLICT (key) DO UPDATE SET data=$2, expire_at=$3`,
		skey, serverData, expireAt)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("upsert server: %v", err))
	}

	// Clear old workers for this server.
	_, _ = tx.Exec(`DELETE FROM asynq_workers WHERE server_key=$1`, skey)

	for _, w := range workers {
		workerData, err := base.EncodeWorkerInfo(w)
		if err != nil {
			continue
		}
		_, _ = tx.Exec(
			`INSERT INTO asynq_workers (server_key, worker_id, data, expire_at)
			 VALUES ($1, $2, $3, $4)`,
			skey, w.ID, workerData, expireAt)
	}
	return tx.Commit()
}

func (b *PGBroker) ClearServerState(host string, pid int, serverID string) error {
	var op errors.Op = "pgbroker.ClearServerState"
	skey := base.ServerInfoKey(host, pid, serverID)

	tx, err := b.db.Begin()
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("begin tx: %v", err))
	}
	defer tx.Rollback()

	_, _ = tx.Exec(`DELETE FROM asynq_workers WHERE server_key=$1`, skey)
	_, _ = tx.Exec(`DELETE FROM asynq_servers WHERE key=$1`, skey)
	return tx.Commit()
}

// pgCancelSub implements base.CancelSubscription using PostgreSQL LISTEN/NOTIFY
// via periodic polling (since database/sql does not expose LISTEN natively).
type pgCancelSub struct {
	db     *sql.DB
	ch     chan string
	done   chan struct{}
	closed chan struct{}
}

func newPGCancelSub(db *sql.DB) *pgCancelSub {
	s := &pgCancelSub{
		db:     db,
		ch:     make(chan string, 100),
		done:   make(chan struct{}),
		closed: make(chan struct{}),
	}
	go s.poll()
	return s
}

// poll periodically checks for cancelation notifications using a polling table.
// Since database/sql does not support LISTEN/NOTIFY natively, we use a
// dedicated table as a notification queue.
func (s *pgCancelSub) poll() {
	defer close(s.closed)

	// Ensure the notification table exists.
	_, _ = s.db.Exec(`
		CREATE TABLE IF NOT EXISTS asynq_cancel_notifications (
			id BIGSERIAL PRIMARY KEY,
			task_id TEXT NOT NULL,
			created_at BIGINT NOT NULL
		)
	`)

	var lastID int64
	// Get the current max ID to skip old notifications.
	_ = s.db.QueryRow(`SELECT COALESCE(MAX(id), 0) FROM asynq_cancel_notifications`).Scan(&lastID)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			rows, err := s.db.Query(
				`SELECT id, task_id FROM asynq_cancel_notifications WHERE id > $1 ORDER BY id`,
				lastID)
			if err != nil {
				continue
			}
			for rows.Next() {
				var id int64
				var taskID string
				if err := rows.Scan(&id, &taskID); err != nil {
					continue
				}
				lastID = id
				select {
				case s.ch <- taskID:
				default:
				}
			}
			rows.Close()

			// Prune old notifications (keep last hour).
			cutoff := time.Now().Unix() - 3600
			_, _ = s.db.Exec(
				`DELETE FROM asynq_cancel_notifications WHERE created_at < $1`, cutoff)
		}
	}
}

func (s *pgCancelSub) Channel() <-chan string { return s.ch }
func (s *pgCancelSub) Close() error {
	close(s.done)
	<-s.closed
	return nil
}

func (b *PGBroker) CancelationPubSub() (base.CancelSubscription, error) {
	return newPGCancelSub(b.db), nil
}

func (b *PGBroker) PublishCancelation(id string) error {
	_, _ = b.db.Exec(`
		CREATE TABLE IF NOT EXISTS asynq_cancel_notifications (
			id BIGSERIAL PRIMARY KEY,
			task_id TEXT NOT NULL,
			created_at BIGINT NOT NULL
		)
	`)
	_, err := b.db.Exec(
		`INSERT INTO asynq_cancel_notifications (task_id, created_at) VALUES ($1, $2)`,
		id, time.Now().Unix())
	return err
}

func (b *PGBroker) WriteResult(qname, taskID string, data []byte) (int, error) {
	var op errors.Op = "pgbroker.WriteResult"
	_, err := b.db.Exec(
		`UPDATE asynq_tasks SET result=$1 WHERE queue=$2 AND id=$3`,
		data, qname, taskID)
	if err != nil {
		return 0, errors.E(op, errors.Unknown, fmt.Sprintf("update result: %v", err))
	}
	return len(data), nil
}

// incrStats increments processed and failed counters.
func (b *PGBroker) incrStats(ctx context.Context, tx *sql.Tx, qname string, t time.Time, processed, failed int64) {
	date := t.UTC().Format("2006-01-02")

	_, _ = tx.ExecContext(ctx,
		`INSERT INTO asynq_stats_daily (queue, date, processed, failed)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (queue, date)
		 DO UPDATE SET processed = asynq_stats_daily.processed + $3,
		               failed = asynq_stats_daily.failed + $4`,
		qname, date, processed, failed)

	_, _ = tx.ExecContext(ctx,
		`INSERT INTO asynq_stats_total (queue, processed, failed)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (queue)
		 DO UPDATE SET processed = CASE
		   WHEN asynq_stats_total.processed >= $4 THEN 1
		   ELSE asynq_stats_total.processed + $2
		 END,
		 failed = CASE
		   WHEN asynq_stats_total.failed >= $4 THEN 1
		   ELSE asynq_stats_total.failed + $3
		 END`,
		qname, processed, failed, int64(math.MaxInt64))
}

// compile-time check
var _ base.Broker = (*PGBroker)(nil)
