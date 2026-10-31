package store

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestOnlyPlainSchemaNamesAreAccepted(t *testing.T) {
	for _, ok := range []string{"chronicle", "chronicle_prod", "_c1", strings.Repeat("a", 63)} {
		if err := ValidSchema(ok); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "Chronicle", "1chronicle", "chron-icle", "chron icle", `x"; drop schema public; --`, "a.b", "pg_catalog", "pg_temp", strings.Repeat("a", 64)} {
		if err := ValidSchema(bad); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}

func TestThePoolFailsFastBoundsItsSizeAndSelectsTheSchema(t *testing.T) {
	cfg, err := PoolConfig("postgres://u:p@db.example.com:5432/app?sslmode=require", "chronicle", 4)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.ConnectTimeout != 5*time.Second || cfg.HealthCheckPeriod != 30*time.Second || cfg.MaxConns != 4 || cfg.AfterConnect == nil {
		t.Fatalf("%+v", cfg)
	}
	// Not a startup parameter: poolers refuse those.
	if _, ok := cfg.ConnConfig.RuntimeParams["search_path"]; ok {
		t.Error("the schema is set as a startup parameter; some poolers refuse that")
	}
	// Without a schema or a size, nothing changes from the default behaviour.
	plain, err := PoolConfig("postgres://u:p@db.example.com/app", "", 0)
	if err != nil || plain.AfterConnect != nil || plain.MaxConns < 4 {
		t.Fatalf("%+v %v", plain, err)
	}
	if _, err := PoolConfig("postgres://u:p@db/app", `x"; drop`, 0); err == nil {
		t.Fatal("a hostile schema name reached the pool")
	}
	if _, err := PoolConfig("not a url", "", 0); err == nil {
		t.Fatal("a malformed URL was accepted")
	}
}

// Against a real database: tables land in the chosen schema and not in public.
func TestPostgresTheSchemaIsWhereTablesAreCreatedAndNotPublic(t *testing.T) {
	url := os.Getenv("CHRONICLE_TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("set CHRONICLE_TEST_POSTGRES_URL to run the PostgreSQL integration test")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "chronicle_pooltest"
	_, _ = admin.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)

	cfg, err := PoolConfig(url, schema, 3)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for i := 0; i < 5; i++ { // more checkouts than connections: every one must have the schema
		var current string
		if err := pool.QueryRow(ctx, `select current_schema()`).Scan(&current); err != nil || current != schema {
			t.Fatalf("connection %d is in schema %q (%v)", i, current, err)
		}
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE pooltest_events (id int)`); err != nil {
		t.Fatal(err)
	}
	var inSchema, inPublic int
	_ = admin.QueryRow(ctx, `select count(*) from information_schema.tables where table_schema=$1 and table_name='pooltest_events'`, schema).Scan(&inSchema)
	_ = admin.QueryRow(ctx, `select count(*) from information_schema.tables where table_schema='public' and table_name='pooltest_events'`).Scan(&inPublic)
	if inSchema != 1 || inPublic != 0 {
		t.Fatalf("in schema %d, in public %d", inSchema, inPublic)
	}
}
