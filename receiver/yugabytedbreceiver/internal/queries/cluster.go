// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/yugabytedbreceiver/internal/queries"

// ============================================================================
// Cluster Topology and Live Query Information
// Queries for tablet server information and currently running queries
// ============================================================================

const (
	// TserversInClustersQuery retrieves tablet server information from the cluster
	TserversInClustersQuery = `
		SELECT
			host,
			cloud,
			region,
			zone
		FROM yb_servers()
		ORDER BY region, zone`

	// LiveLongQueriesQuery retrieves all currently running queries with geographic distribution
	LiveLongQueriesQuery = `
		SELECT
			datname,
			pid,
			usename,
			application_name,
			state,
			query,
			"gv$host" as host,
			"gv$cloud" as cloud,
			"gv$zone" as zone,
			"gv$region" as region,
			ROUND(EXTRACT(EPOCH FROM (now() - query_start))::numeric, 2) AS txn_duration_sec
		FROM gv_history."gv$pg_stat_activity"
		WHERE state = 'active'
			AND backend_type = 'client backend'
			AND query NOT LIKE '%pg_stat_activity%'
		ORDER BY (now() - query_start) DESC`
)
