// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package queries contains SQL queries for the YugabyteDB receiver.
// Queries are organized by their data source and purpose:
//   - local.go: Single-node queries against pg_stat_activity
//   - global_activity.go: Cluster-wide queries against gv$pg_stat_activity
//   - global_statements.go: Statement statistics from global_pg_stat_statements
//   - cluster.go: Cluster topology and live query information
package queries // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/yugabytedbreceiver/internal/queries"
