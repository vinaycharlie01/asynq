// Copyright 2024 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"database/sql"

	"github.com/hibiken/asynq/internal/pgbroker"
)

// NewClientFromDB returns a new Client backed by PostgreSQL.
// The caller must import a PostgreSQL driver (e.g. github.com/lib/pq
// or github.com/jackc/pgx/v5/stdlib) and pass an open *sql.DB.
//
// The underlying *sql.DB will not be closed by Asynq; the caller is
// responsible for closing it.
func NewClientFromDB(db *sql.DB) (*Client, error) {
	broker, err := pgbroker.New(db)
	if err != nil {
		return nil, err
	}
	return &Client{broker: broker, sharedConnection: true}, nil
}

// NewServerFromDB returns a new Server backed by PostgreSQL.
// The caller must import a PostgreSQL driver (e.g. github.com/lib/pq
// or github.com/jackc/pgx/v5/stdlib) and pass an open *sql.DB.
//
// The underlying *sql.DB will not be closed by Asynq; the caller is
// responsible for closing it.
func NewServerFromDB(db *sql.DB, cfg Config) (*Server, error) {
	broker, err := pgbroker.New(db)
	if err != nil {
		return nil, err
	}
	srv := newServerFromBroker(broker, cfg)
	srv.sharedConnection = true
	return srv, nil
}
