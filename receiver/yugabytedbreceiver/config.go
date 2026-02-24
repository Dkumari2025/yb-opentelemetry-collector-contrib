// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package yugabytedbreceiver

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
)

// Config defines the configuration for the YugabyteDB receiver
type Config struct {
	confighttp.ServerConfig `mapstructure:",squash"`

	// Connection parameters
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
}

func createDefaultConfig() component.Config {
	return &Config{
		Host:     "localhost",
		Port:     5433,
		User:     "yugabyte",
		Password: "yugabyte",
		Database: "yugabyte",
	}
}
