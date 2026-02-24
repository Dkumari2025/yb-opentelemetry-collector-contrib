#!/bin/bash
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

################################################################################
# YugabyteDB Global View Setup - Docker Compose Example
#
# Simplified script for docker-compose environment setup
################################################################################

set -e

# Docker-compose specific defaults
YB_HOST="${YB_HOST:-localhost}"
YB_PORT="${YB_PORT:-5433}"
YB_ADMIN_USER="${YB_ADMIN_USER:-yugabyte}"
YB_ADMIN_PASSWORD="${YB_ADMIN_PASSWORD:-yugabyte}"
YB_DATABASE="${YB_DATABASE:-yugabyte}"
MONITORING_USER="${MONITORING_USER:-nr_monitor}"
MONITORING_PASSWORD="${MONITORING_PASSWORD:-monitor123}"

echo "=================================================="
echo "YugabyteDB Global View Setup (Docker Compose)"
echo "=================================================="
echo "Host: ${YB_HOST}:${YB_PORT}"
echo "Database: ${YB_DATABASE}"
echo "Monitoring User: ${MONITORING_USER}"
echo "=================================================="
echo ""

# Wait for YugabyteDB to be ready
echo "Waiting for YugabyteDB to be ready..."
until psql "postgresql://${YB_ADMIN_USER}:${YB_ADMIN_PASSWORD}@${YB_HOST}:${YB_PORT}/${YB_DATABASE}" -c "SELECT 1;" > /dev/null 2>&1; do
    echo "YugabyteDB is unavailable - sleeping"
    sleep 2
done
echo "YugabyteDB is ready!"
echo ""

# Determine the scripts directory
# This script is in: receiver/yugabytedbreceiver/examples/docker-compose/
# Scripts are in: receiver/yugabytedbreceiver/scripts/
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(cd "${SCRIPT_DIR}/../../scripts" && pwd)"

# Verify the main script exists
if [ ! -f "${SCRIPTS_DIR}/setup_yugabytedb_monitoring.sh" ]; then
    echo "ERROR: Setup script not found at: ${SCRIPTS_DIR}/setup_yugabytedb_monitoring.sh"
    exit 1
fi

# Run the main setup script
echo "Running Global View setup from: ${SCRIPTS_DIR}"
export YB_HOST
export YB_PORT
export YB_ADMIN_USER
export YB_ADMIN_PASSWORD
export YB_DATABASE
export MONITORING_USER
export MONITORING_PASSWORD

# Call the main setup script
exec "${SCRIPTS_DIR}/setup_yugabytedb_monitoring.sh"