package observability

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type dbTraceKey struct{}
type dbStatementKey struct{}

type DBQueryTracer struct{}

func NewDBQueryTracer() *DBQueryTracer {
	return &DBQueryTracer{}
}

func (t *DBQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	ctx = context.WithValue(ctx, dbTraceKey{}, time.Now())
	return context.WithValue(ctx, dbStatementKey{}, data.SQL)
}

func (t *DBQueryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	start, _ := ctx.Value(dbTraceKey{}).(time.Time)
	statement, _ := ctx.Value(dbStatementKey{}).(string)
	RecordDBQuery(statement, data.Err, time.Since(start))
}
