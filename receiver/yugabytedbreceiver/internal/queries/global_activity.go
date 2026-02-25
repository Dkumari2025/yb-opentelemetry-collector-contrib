// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

// ============================================================================
// Global View Queries (cluster-wide mode, use_global_view: true)
// Query gv$pg_stat_activity view that aggregates data from all nodes
// Includes node metadata: gv$host, gv$zone, gv$region, gv$cloud
// ============================================================================

const (
	// GlobalViewRunningQueriesQuery counts active queries per node
	GlobalViewRunningQueriesQuery = `
		SELECT
			"gv$host",
			"gv$zone",
			"gv$region",
			"gv$cloud",
			count(*) as count
		FROM gv_history."gv$pg_stat_activity"
		WHERE state = 'active'
		GROUP BY "gv$host", "gv$zone", "gv$region", "gv$cloud"`

	// GlobalViewActiveConnectionsQuery counts total connections per node
	GlobalViewActiveConnectionsQuery = `
		SELECT
			"gv$host",
			"gv$zone",
			"gv$region",
			"gv$cloud",
			count(*) as count
		FROM gv_history."gv$pg_stat_activity"
		GROUP BY "gv$host", "gv$zone", "gv$region", "gv$cloud"`

	// GlobalViewConnectionsByStateAndUserQuery retrieves connection counts by state, user, and node
	GlobalViewConnectionsByStateAndUserQuery = `
		SELECT
			"gv$host",
			"gv$zone",
			"gv$region",
			"gv$cloud",
			COALESCE(state, 'unknown') as state,
			COALESCE(usename, 'unknown') as usename,
			count(*) as count
		FROM gv_history."gv$pg_stat_activity"
		GROUP BY "gv$host", "gv$zone", "gv$region", "gv$cloud", state, usename`

	// GlobalViewActiveUserCountQuery counts unique active users per node with client backend connections
	GlobalViewActiveUserCountQuery = `
		SELECT
			"gv$host",
			"gv$zone",
			"gv$region",
			"gv$cloud",
			COALESCE(usename, 'unknown') as usename,
			COUNT(*) as user_session_count
		FROM gv_history."gv$pg_stat_activity"
		WHERE state = 'active'
		AND backend_type = 'client backend'
		GROUP BY "gv$host", "gv$zone", "gv$region", "gv$cloud", usename`

	// GlobalViewActiveUserCountPerNodeQuery counts unique active users per node (alternative with all metadata)
	GlobalViewActiveUserCountPerNodeQuery = `
		SELECT
			usename,
			"gv$host",
			"gv$zone",
			"gv$region",
			"gv$cloud",
			COUNT(*) AS unique_active_users_counts
		FROM gv_history."gv$pg_stat_activity"
		WHERE state = 'active'
		AND backend_type = 'client backend'
		GROUP BY usename, "gv$host", "gv$zone", "gv$region", "gv$cloud"`

	// GlobalViewActiveConnectionsByUsernameQuery counts active connections per username
	GlobalViewActiveConnectionsByUsernameQuery = `
		SELECT
			usename AS username,
			count(*)
		FROM gv_history."gv$pg_stat_activity"
		WHERE state = 'active'
		AND backend_type = 'client backend'
		AND "gv$host" IS NOT NULL
		GROUP BY usename`
)
