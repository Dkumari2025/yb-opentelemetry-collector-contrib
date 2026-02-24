// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/yugabytedbreceiver/internal/queries"

// ============================================================================
// Local Queries (single-node mode, use_global_view: false)
// Query pg_stat_activity table on the connected node only
// ============================================================================

const (
	// RunningQueriesQuery counts the number of currently active queries
	RunningQueriesQuery = `SELECT count(*) FROM pg_stat_activity WHERE state = 'active'`

	// ActiveConnectionsQuery counts the total number of active connections
	ActiveConnectionsQuery = `SELECT count(*) FROM pg_stat_activity`

	// ConnectionsByStateAndUserQuery retrieves connection counts grouped by connection state and user
	ConnectionsByStateAndUserQuery = `
		SELECT
			COALESCE(state, 'unknown') as state,
			COALESCE(usename, 'unknown') as usename,
			count(*) as count
		FROM pg_stat_activity
		GROUP BY state, usename`

	// ActiveUserCountQuery counts unique active users with client backend connections
	ActiveUserCountQuery = `
		SELECT
			COALESCE(usename, 'unknown') as usename
		FROM pg_stat_activity
		WHERE state = 'active'
		AND backend_type = 'client backend'
		GROUP BY usename`
)
