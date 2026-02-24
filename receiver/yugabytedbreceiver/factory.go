// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package yugabytedbreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/yugabytedbreceiver"

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/receiver"
)

var typeStr = component.MustNewType("yugabytedb")

// NewFactory creates a new YugabyteDB receiver factory.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		typeStr,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelAlpha),
	)
}
