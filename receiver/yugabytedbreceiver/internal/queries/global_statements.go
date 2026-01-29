// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// ============================================================================
// Global Statement Statistics Queries
// Query global_pg_stat_statements for query performance data
// ============================================================================

const (
	// LatencyCalculationQuery retrieves query statistics from the last 15 minutes
	LatencyCalculationQuery = `
		SELECT
			query,
			calls,
			total_time,
			mean_time,
			COALESCE(yb_get_percentile(yb_latency_histogram, 99), 0) AS p99,
			COALESCE(yb_get_percentile(yb_latency_histogram, 95), 0) AS p95,
			COALESCE(yb_get_percentile(yb_latency_histogram, 90), 0) AS p90
		FROM gv_history.global_pg_stat_statements
		WHERE snapshot_time >= NOW() - '15 minutes'::interval
		LIMIT 10`

	// TotalQPMQuery calculates total queries per minute across all statement types
	TotalQPMQuery = `
		SELECT
			SUM(calls) / 15.0 AS qpm
		FROM gv_history.global_pg_stat_statements
		WHERE snapshot_time >= NOW() - '15 minutes'::interval`

	// GlobalViewStatementTypeStats aggregates query statistics by statement type (INSERT, SELECT, UPDATE, DELETE, UPSERT, OTHER)
	// Returns only latency percentiles (P90/P95/P99) - QPM is collected separately via TotalQPMQuery
	GlobalViewStatementTypeStats = `
		SELECT
			CASE
				WHEN query ILIKE 'WITH upsert%' THEN 'UPSERT'
				WHEN query ILIKE 'INSERT%' THEN 'INSERT'
				WHEN query ILIKE 'SELECT%' THEN 'SELECT'
				WHEN query ILIKE 'UPDATE%' THEN 'UPDATE'
				WHEN query ILIKE 'DELETE%' THEN 'DELETE'
				ELSE 'OTHER'
			END AS statement_type,
			SUM(calls) as total_calls,
			AVG(COALESCE(yb_get_percentile(yb_latency_histogram, 90), 0)) AS p90,
			AVG(COALESCE(yb_get_percentile(yb_latency_histogram, 95), 0)) AS p95,
			AVG(COALESCE(yb_get_percentile(yb_latency_histogram, 99), 0)) AS p99
		FROM gv_history.global_pg_stat_statements
		WHERE snapshot_time >= NOW() - '15 minutes'::interval
		GROUP BY 1`
)
