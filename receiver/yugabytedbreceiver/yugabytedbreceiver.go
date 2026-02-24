// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
package yugabytedbreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/yugabytedbreceiver"

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/yugabytedbreceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/yugabytedbreceiver/internal/queries"
)

// yugabytedbReceiver collects metrics from YugabyteDB
type yugabytedbReceiver struct {
	config                       *Config
	consumer                     consumer.Metrics
	cancel                       context.CancelFunc
	metricsBuilder               *metadata.MetricsBuilder
	logger                       *zap.Logger
	lastStatementStatsCollection time.Time
}

// connectionMetric represents connection count grouped by state and user
type connectionMetric struct {
	state string
	user  string
	count int64
}

// createMetricsReceiver creates a new YugabyteDB metrics receiver
func createMetricsReceiver(_ context.Context, settings receiver.Settings, cfg component.Config, consumer consumer.Metrics) (receiver.Metrics, error) {
	c := cfg.(*Config)
	mbConfig := metadata.DefaultMetricsBuilderConfig()
	mb := metadata.NewMetricsBuilder(mbConfig, settings)
	return &yugabytedbReceiver{
		config:         c,
		consumer:       consumer,
		metricsBuilder: mb,
		logger:         settings.Logger,
	}, nil
}

// Start begins the metric collection process
func (r *yugabytedbReceiver) Start(ctx context.Context, _ component.Host) error {
	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	go r.scrapeLoop(ctx)
	return nil
}

// Shutdown stops the metric collection process
func (r *yugabytedbReceiver) Shutdown(_ context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}

// scrapeLoop continuously collects metrics at regular intervals
func (r *yugabytedbReceiver) scrapeLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.collectMetrics(ctx)
		}
	}
}

// collectMetrics gathers all metrics from YugabyteDB and emits them
func (r *yugabytedbReceiver) collectMetrics(ctx context.Context) {
	db, err := r.connectToDatabase()
	if err != nil {
		r.logger.Error("failed to connect to YugabyteDB", zap.Error(err))
		return
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			r.logger.Error("failed to close database connection", zap.Error(closeErr))
		}
	}()
	now := pcommon.NewTimestampFromTime(time.Now())

	// Collect metrics from Global Views (every 10 seconds)
	r.collectRunningQueries(ctx, db, now)
	r.collectActiveConnections(ctx, db, now)
	r.collectConnectionsByStateAndUser(ctx, db, now)
	r.collectActiveUserCount(ctx, db, now)
	r.collectActiveConnectionsByUsername(ctx, db, now)
	r.collectTserversInClusters(ctx, db, now)
	r.collectLiveLongQueries(ctx, db, now)

	// Collect statement stats every 15 minutes (matches query window)
	if time.Since(r.lastStatementStatsCollection) >= 15*time.Minute {
		r.collectQueryStatements(ctx, db, now)    // Top 10 slow queries + Total QPM
		r.collectStatementTypeStats(ctx, db, now) // P90/P95/P99 by statement type
		r.lastStatementStatsCollection = time.Now()
	}

	// Emit all metrics to the consumer
	r.emitMetrics(ctx)
}

// connectToDatabase establishes a connection to YugabyteDB
func (r *yugabytedbReceiver) connectToDatabase() (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		r.config.Host, r.config.Port, r.config.User, r.config.Password, r.config.Database)
	return sql.Open("postgres", dsn)
}

// normalizeConnectionState normalizes PostgreSQL connection states to our metric format
func (r *yugabytedbReceiver) normalizeConnectionState(state string) string {
	switch state {
	case "idle in transaction", "idle in transaction (aborted)":
		return "idle_in_transaction"
	case "":
		return "unknown"
	default:
		return state
	}
}

// collectRunningQueries collects running queries count from Global Views
func (r *yugabytedbReceiver) collectRunningQueries(ctx context.Context, db *sql.DB, now pcommon.Timestamp) {
	rows, err := db.QueryContext(ctx, queries.GlobalViewRunningQueriesQuery)
	if err != nil {
		r.logger.Error("failed to query running queries", zap.Error(err))
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			r.logger.Error("failed to close rows", zap.Error(closeErr))
		}
	}()

	totalCount := int64(0)
	for rows.Next() {
		var host, zone, region, cloud string
		var count int64
		err := rows.Scan(&host, &zone, &region, &cloud, &count)
		if err != nil {
			r.logger.Error("failed to scan row", zap.Error(err))
			continue
		}
		totalCount += count
		r.logger.Debug("running queries", zap.String("host", host), zap.Int64("count", count))
	}

	if err := rows.Err(); err != nil {
		r.logger.Error("error iterating rows", zap.Error(err))
		return
	}

	r.metricsBuilder.RecordYugabytedbPgStatActivityRunningQueriesDataPoint(now, totalCount)
	r.logger.Debug("collected running queries", zap.Int64("total", totalCount))
}

// collectActiveConnections collects active connections count from Global Views
func (r *yugabytedbReceiver) collectActiveConnections(ctx context.Context, db *sql.DB, now pcommon.Timestamp) {
	rows, err := db.QueryContext(ctx, queries.GlobalViewActiveConnectionsQuery)
	if err != nil {
		r.logger.Error("failed to query active connections", zap.Error(err))
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			r.logger.Error("failed to close rows", zap.Error(closeErr))
		}
	}()

	totalCount := int64(0)
	for rows.Next() {
		var host, zone, region, cloud string
		var count int64
		err := rows.Scan(&host, &zone, &region, &cloud, &count)
		if err != nil {
			r.logger.Error("failed to scan row", zap.Error(err))
			continue
		}
		totalCount += count
		r.logger.Debug("active connections", zap.String("host", host), zap.Int64("count", count))
	}

	if err := rows.Err(); err != nil {
		r.logger.Error("error iterating rows", zap.Error(err))
		return
	}

	r.metricsBuilder.RecordYugabytedbPgStatActivityActiveConnectionsDataPoint(now, totalCount)
	r.logger.Debug("collected active connections", zap.Int64("total", totalCount))
}

// collectConnectionsByStateAndUser collects connection counts by state and user from Global Views
func (r *yugabytedbReceiver) collectConnectionsByStateAndUser(ctx context.Context, db *sql.DB, now pcommon.Timestamp) {
	rows, err := db.QueryContext(ctx, queries.GlobalViewConnectionsByStateAndUserQuery)
	if err != nil {
		r.logger.Error("failed to query connections by state and user", zap.Error(err))
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			r.logger.Error("failed to close rows", zap.Error(closeErr))
		}
	}()

	// Aggregate by state and user across all nodes
	aggregates := make(map[string]map[string]int64) // state -> user -> count

	for rows.Next() {
		var host, zone, region, cloud, state, user string
		var count int64
		err := rows.Scan(&host, &zone, &region, &cloud, &state, &user, &count)
		if err != nil {
			r.logger.Error("failed to scan row", zap.Error(err))
			continue
		}

		normalizedState := r.normalizeConnectionState(state)
		if aggregates[normalizedState] == nil {
			aggregates[normalizedState] = make(map[string]int64)
		}
		aggregates[normalizedState][user] += count

		r.logger.Debug("connection",
			zap.String("host", host),
			zap.String("state", normalizedState),
			zap.String("user", user),
			zap.Int64("count", count))
	}

	if err := rows.Err(); err != nil {
		r.logger.Error("error iterating rows", zap.Error(err))
		return
	}

	// Record aggregated metrics
	for state, users := range aggregates {
		for user, count := range users {
			r.metricsBuilder.RecordYugabytedbConnectionCountDataPoint(now, count, state, user)
		}
	}

	r.logger.Debug("collected connections by state and user", zap.Int("state_count", len(aggregates)))
}

// collectActiveUserCount collects unique active user count from Global Views with per-node resource attributes
func (r *yugabytedbReceiver) collectActiveUserCount(ctx context.Context, db *sql.DB, now pcommon.Timestamp) {
	rows, err := db.QueryContext(ctx, queries.GlobalViewActiveUserCountPerNodeQuery)
	if err != nil {
		r.logger.Error("failed to query active user count", zap.Error(err))
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			r.logger.Error("failed to close rows", zap.Error(closeErr))
		}
	}()

	// Collect per-node metrics with resource attributes
	for rows.Next() {
		var user, host, zone, region, cloud string
		var count int64
		err := rows.Scan(&user, &host, &zone, &region, &cloud, &count)
		if err != nil {
			r.logger.Error("failed to scan row", zap.Error(err))
			continue
		}

		// Create resource builder with node metadata
		rb := r.metricsBuilder.NewResourceBuilder()
		rb.SetYugabytedbNodeHost(host)
		rb.SetYugabytedbNodeZone(zone)
		rb.SetYugabytedbNodeRegion(region)
		rb.SetYugabytedbNodeCloud(cloud)

		// Record metric for this user on this specific node
		r.metricsBuilder.RecordYugabytedbActiveUsersCountDataPoint(now, count, user)

		// Emit metrics with node resource attributes
		r.metricsBuilder.EmitForResource(metadata.WithResource(rb.Emit()))

		r.logger.Debug("active user session per node",
			zap.String("host", host),
			zap.String("zone", zone),
			zap.String("region", region),
			zap.String("cloud", cloud),
			zap.String("user", user),
			zap.Int64("session_count", count))
	}

	if err := rows.Err(); err != nil {
		r.logger.Error("error iterating rows", zap.Error(err))
		return
	}

	r.logger.Debug("collected active user count per node")
}

// collectActiveConnectionsByUsername collects active connection counts aggregated by username across all nodes
func (r *yugabytedbReceiver) collectActiveConnectionsByUsername(ctx context.Context, db *sql.DB, now pcommon.Timestamp) {
	rows, err := db.QueryContext(ctx, queries.GlobalViewActiveConnectionsByUsernameQuery)
	if err != nil {
		r.logger.Error("failed to query active connections by username", zap.Error(err))
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			r.logger.Error("failed to close rows", zap.Error(closeErr))
		}
	}()

	for rows.Next() {
		var username string
		var count int64
		err := rows.Scan(&username, &count)
		if err != nil {
			r.logger.Error("failed to scan row", zap.Error(err))
			continue
		}

		// Record metric for this username aggregated across all nodes
		r.metricsBuilder.RecordYugabytedbActiveUsersCountDataPoint(now, count, username)

		r.logger.Debug("active connections by username",
			zap.String("username", username),
			zap.Int64("count", count))
	}

	if err := rows.Err(); err != nil {
		r.logger.Error("error iterating rows", zap.Error(err))
		return
	}

	r.logger.Debug("collected active connections by username")
}

// collectQueryStatements collects query statistics from global_pg_stat_statements
func (r *yugabytedbReceiver) collectQueryStatements(ctx context.Context, db *sql.DB, now pcommon.Timestamp) {
	rows, err := db.QueryContext(ctx, queries.LatencyCalculationQuery)
	if err != nil {
		r.logger.Error("failed to query statement statistics", zap.Error(err))
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			r.logger.Error("failed to close rows", zap.Error(closeErr))
		}
	}()

	for rows.Next() {
		var query string
		var calls int64
		var totalTime, meanTime sql.NullFloat64
		var p99, p95, p90 sql.NullFloat64

		err := rows.Scan(&query, &calls, &totalTime, &meanTime, &p99, &p95, &p90)
		if err != nil {
			r.logger.Error("failed to scan row", zap.Error(err))
			continue
		}

		// Record metrics with NULL-safe handling
		r.metricsBuilder.RecordYugabytedbQueryCallsDataPoint(now, calls, query)

		if totalTime.Valid {
			r.metricsBuilder.RecordYugabytedbQueryTotalTimeDataPoint(now, totalTime.Float64, query)
		}
		if meanTime.Valid {
			r.metricsBuilder.RecordYugabytedbQueryMeanTimeDataPoint(now, meanTime.Float64, query)
		}
		if p99.Valid {
			r.metricsBuilder.RecordYugabytedbQueryLatencyP99DataPoint(now, p99.Float64, query)
		}
		if p95.Valid {
			r.metricsBuilder.RecordYugabytedbQueryLatencyP95DataPoint(now, p95.Float64, query)
		}
		if p90.Valid {
			r.metricsBuilder.RecordYugabytedbQueryLatencyP90DataPoint(now, p90.Float64, query)
		}

		r.logger.Debug("slow query statistics",
			zap.String("query", query),
			zap.Int64("calls", calls),
			zap.Float64("total_time_ms", totalTime.Float64),
			zap.Float64("mean_time_ms", meanTime.Float64))
	}

	if err := rows.Err(); err != nil {
		r.logger.Error("error iterating rows", zap.Error(err))
		return
	}

	r.logger.Debug("collected query statements")

	// Also collect total QPM in the same 15-minute collection cycle
	row := db.QueryRowContext(ctx, queries.TotalQPMQuery)
	var qpm sql.NullFloat64
	if err := row.Scan(&qpm); err != nil {
		r.logger.Error("failed to query total QPM", zap.Error(err))
	} else if qpm.Valid {
		r.metricsBuilder.RecordYugabytedbTotalQpmDataPoint(now, qpm.Float64)
		r.logger.Debug("collected total QPM", zap.Float64("qpm", qpm.Float64))
	}
}

// collectStatementTypeStats collects aggregated query statistics by statement type
func (r *yugabytedbReceiver) collectStatementTypeStats(ctx context.Context, db *sql.DB, now pcommon.Timestamp) {
	rows, err := db.QueryContext(ctx, queries.GlobalViewStatementTypeStats)
	if err != nil {
		r.logger.Error("failed to query statement type statistics", zap.Error(err))
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			r.logger.Error("failed to close rows", zap.Error(closeErr))
		}
	}()

	for rows.Next() {
		var statementType string
		var totalCalls int64
		var p90, p95, p99 sql.NullFloat64

		err := rows.Scan(&statementType, &totalCalls, &p90, &p95, &p99)
		if err != nil {
			r.logger.Error("failed to scan row", zap.Error(err))
			continue
		}

		// Record metrics with NULL-safe handling
		r.metricsBuilder.RecordYugabytedbStatementCallsDataPoint(now, totalCalls, statementType)

		if p90.Valid {
			r.metricsBuilder.RecordYugabytedbStatementLatencyP90DataPoint(now, p90.Float64, statementType)
		}
		if p95.Valid {
			r.metricsBuilder.RecordYugabytedbStatementLatencyP95DataPoint(now, p95.Float64, statementType)
		}
		if p99.Valid {
			r.metricsBuilder.RecordYugabytedbStatementLatencyP99DataPoint(now, p99.Float64, statementType)
		}

		r.logger.Debug("statement type statistics",
			zap.String("statement_type", statementType),
			zap.Int64("total_calls", totalCalls))
	}

	if err := rows.Err(); err != nil {
		r.logger.Error("error iterating rows", zap.Error(err))
		return
	}

	r.logger.Debug("collected statement type statistics")
}

// collectTserversInClusters collects tablet server information from the cluster
func (r *yugabytedbReceiver) collectTserversInClusters(ctx context.Context, db *sql.DB, now pcommon.Timestamp) {
	rows, err := db.QueryContext(ctx, queries.TserversInClustersQuery)
	if err != nil {
		r.logger.Error("failed to query tservers in clusters", zap.Error(err))
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			r.logger.Error("failed to close rows", zap.Error(closeErr))
		}
	}()

	tserverCount := 0
	for rows.Next() {
		var host, cloud, region, zone string

		err := rows.Scan(&host, &cloud, &region, &zone)
		if err != nil {
			r.logger.Error("failed to scan row", zap.Error(err))
			continue
		}

		tserverCount++

		// Create resource builder with tserver metadata (host, cloud, region, zone)
		rb := r.metricsBuilder.NewResourceBuilder()
		rb.SetYugabytedbNodeHost(host)
		rb.SetYugabytedbNodeCloud(cloud)
		rb.SetYugabytedbNodeRegion(region)
		rb.SetYugabytedbNodeZone(zone)

		// Record status metric for this tserver (1 = available/present)
		r.metricsBuilder.RecordYugabytedbTserverStatusDataPoint(now, int64(1))

		// Emit metrics with tserver resource attributes
		r.metricsBuilder.EmitForResource(metadata.WithResource(rb.Emit()))

		r.logger.Debug("tserver in cluster",
			zap.String("host", host),
			zap.String("cloud", cloud),
			zap.String("region", region),
			zap.String("zone", zone))
	}

	if err := rows.Err(); err != nil {
		r.logger.Error("error iterating rows", zap.Error(err))
		return
	}

	// Record total tserver count as a metric
	r.metricsBuilder.RecordYugabytedbTserverCountDataPoint(now, int64(tserverCount))
	r.logger.Debug("collected tservers in clusters", zap.Int("tserver_count", tserverCount))
}

// collectLiveLongQueries collects all currently running queries with geographic distribution
func (r *yugabytedbReceiver) collectLiveLongQueries(ctx context.Context, db *sql.DB, now pcommon.Timestamp) {
	rows, err := db.QueryContext(ctx, queries.LiveLongQueriesQuery)
	if err != nil {
		r.logger.Error("failed to query live queries", zap.Error(err))
		return
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			r.logger.Error("failed to close rows", zap.Error(closeErr))
		}
	}()

	queryCount := 0
	longQueryCount := 0
	for rows.Next() {
		var datname, usename, applicationName, state, query, host, cloud, zone, region string
		var pid int32
		var txnDurationSec float64

		err := rows.Scan(&datname, &pid, &usename, &applicationName, &state, &query, &host, &cloud, &zone, &region, &txnDurationSec)
		if err != nil {
			r.logger.Error("failed to scan row", zap.Error(err))
			continue
		}

		queryCount++

		// Count queries running longer than 5 seconds
		if txnDurationSec > 5.0 {
			longQueryCount++
		}

		// Create resource builder with node metadata (host, cloud, zone, region)
		rb := r.metricsBuilder.NewResourceBuilder()
		rb.SetYugabytedbNodeHost(host)
		rb.SetYugabytedbNodeCloud(cloud)
		rb.SetYugabytedbNodeZone(zone)
		rb.SetYugabytedbNodeRegion(region)

		// Record metric for each running query with its duration and username
		r.metricsBuilder.RecordYugabytedbPgStatActivityQueryDurationDataPoint(now, txnDurationSec, query, datname, applicationName, int64(pid), usename)

		// Also record long query duration for queries > 5 seconds (backward compatibility)
		if txnDurationSec > 5.0 {
			r.metricsBuilder.RecordYugabytedbLongQueryDurationDataPoint(now, txnDurationSec, query, datname, applicationName, int64(pid))
		}

		// Emit metrics with node resource attributes
		r.metricsBuilder.EmitForResource(metadata.WithResource(rb.Emit()))

		r.logger.Debug("active query",
			zap.String("database", datname),
			zap.Int32("pid", pid),
			zap.String("user", usename),
			zap.String("application_name", applicationName),
			zap.String("query", query),
			zap.String("host", host),
			zap.String("cloud", cloud),
			zap.String("zone", zone),
			zap.String("region", region),
			zap.Float64("query_duration_sec", txnDurationSec))
	}

	if err := rows.Err(); err != nil {
		r.logger.Error("error iterating rows", zap.Error(err))
		return
	}

	// Record total count of long-running queries (> 5 seconds)
	r.metricsBuilder.RecordYugabytedbLongQueryCountDataPoint(now, int64(longQueryCount))
	r.logger.Debug("collected active queries", zap.Int("active_query_count", queryCount), zap.Int("long_query_count", longQueryCount))
}

// emitMetrics sends all collected metrics to the consumer
func (r *yugabytedbReceiver) emitMetrics(ctx context.Context) {
	metrics := r.metricsBuilder.Emit()
	if err := r.consumer.ConsumeMetrics(ctx, metrics); err != nil {
		r.logger.Error("failed to consume metrics", zap.Error(err))
	}
}
