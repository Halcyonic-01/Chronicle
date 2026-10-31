package store

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var schemaName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// ValidSchema says whether name can be used as Chronicle's schema: a plain
// lower-case identifier that is not one of PostgreSQL's own. Anything else is
// refused rather than quoted, because the name also reaches a shell script.
func ValidSchema(name string) error {
	switch {
	case !schemaName.MatchString(name):
		return fmt.Errorf("POSTGRES_SCHEMA %q is not a plain lower-case identifier (letters, digits, underscore; at most 63)", name)
	case strings.HasPrefix(name, "pg_"):
		return fmt.Errorf("POSTGRES_SCHEMA %q: names starting with pg_ are reserved by PostgreSQL", name)
	}
	return nil
}

// PoolConfig is the PostgreSQL pool Chronicle runs on.
//
//   - A connection that cannot be made fails in seconds, not after the operating
//     system gives up on the dial.
//   - schema, when set, is where Chronicle's tables live. It is selected with SET
//     search_path after each connection opens (not as a startup option, which
//     connection poolers such as Supabase's refuse), so every query lands there
//     and nothing is created in public. On a managed database that publishes
//     public through an API this keeps Chronicle's record out of it.
//   - maxConns bounds the pool (0 keeps pgx's default); managed databases allow
//     few connections, and every replica holds its own pool.
func PoolConfig(url, schema string, maxConns int) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.HealthCheckPeriod = 30 * time.Second
	if maxConns > 0 {
		cfg.MaxConns = int32(maxConns)
	}
	if schema != "" {
		if err := ValidSchema(schema); err != nil {
			return nil, err
		}
		statement := "SET search_path TO " + pgx.Identifier{schema}.Sanitize()
		cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, statement)
			return err
		}
	}
	return cfg, nil
}
