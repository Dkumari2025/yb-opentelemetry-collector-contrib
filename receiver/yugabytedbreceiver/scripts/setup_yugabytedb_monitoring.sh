#!/bin/bash
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

################################################################################
# YugabyteDB Global View Setup Script for OpenTelemetry Monitoring
#
# Usage: ./setup_yugabytedb_monitoring.sh
#
# Environment Variables:
#   YB_HOST, YB_PORT, YB_ADMIN_USER, YB_ADMIN_PASSWORD, YB_DATABASE
################################################################################

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_header() {
    echo -e "\n${CYAN}${BOLD}=========================================="
    echo -e "$1"
    echo -e "==========================================${NC}\n"
}

# Print banner
echo -e "${CYAN}${BOLD}"
echo "╔════════════════════════════════════════╗"
echo "║  YugabyteDB Global View Setup          ║"
echo "║  OpenTelemetry Monitoring              ║"
echo "╚════════════════════════════════════════╝"
echo -e "${NC}"

# Prompt for credentials if not set
log_header "Connection Configuration"

if [ -z "$YB_HOST" ]; then
    read -p "YugabyteDB host [localhost]: " YB_HOST
    YB_HOST="${YB_HOST:-localhost}"
else
    log_info "Using host from environment: ${YB_HOST}"
fi

if [ -z "$YB_PORT" ]; then
    read -p "YugabyteDB port [5433]: " YB_PORT
    YB_PORT="${YB_PORT:-5433}"
else
    log_info "Using port from environment: ${YB_PORT}"
fi

if [ -z "$YB_ADMIN_USER" ]; then
    read -p "Admin username [yugabyte]: " YB_ADMIN_USER
    YB_ADMIN_USER="${YB_ADMIN_USER:-yugabyte}"
else
    log_info "Using admin user from environment: ${YB_ADMIN_USER}"
fi

if [ -z "$YB_ADMIN_PASSWORD" ]; then
    read -sp "Admin password [yugabyte]: " YB_ADMIN_PASSWORD
    echo
    YB_ADMIN_PASSWORD="${YB_ADMIN_PASSWORD:-yugabyte}"
else
    log_info "Using admin password from environment: ********"
fi

if [ -z "$YB_DATABASE" ]; then
    read -p "Database name [yugabyte]: " YB_DATABASE
    YB_DATABASE="${YB_DATABASE:-yugabyte}"
else
    log_info "Using database from environment: ${YB_DATABASE}"
fi

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Test connection
log_header "Validating Connection"
log_info "Testing connection to ${CYAN}${YB_HOST}:${YB_PORT}${NC}..."

if PGPASSWORD="${YB_ADMIN_PASSWORD}" psql -h "${YB_HOST}" -p "${YB_PORT}" -U "${YB_ADMIN_USER}" -d "${YB_DATABASE}" -c "SELECT 1;" > /dev/null 2>&1; then
    log_success "Successfully connected to YugabyteDB"
else
    log_error "Failed to connect to YugabyteDB"
    log_error "Please check your credentials and connection details"
    exit 1
fi

# Run SQL script for Global Views
log_header "Setting Up Global Views"
log_info "Creating gv\$ database..."
log_info "Setting up Foreign Data Wrappers..."
log_info "Creating foreign servers for cluster nodes..."
log_info "Importing foreign schemas..."
log_info "Creating Global Views..."

if PGPASSWORD="${YB_ADMIN_PASSWORD}" psql -h "${YB_HOST}" -p "${YB_PORT}" -U "${YB_ADMIN_USER}" -d "${YB_DATABASE}" -f "${SCRIPT_DIR}/setup_global_views.sql" > /dev/null 2>&1; then
    log_success "Global Views setup completed"
else
    log_error "Failed to setup Global Views"
    exit 1
fi

# Create monitoring user
log_header "Creating Monitoring User"
log_info "Setting up read-only monitoring user..."
log_info "Granting permissions on Global Views..."

if PGPASSWORD="${YB_ADMIN_PASSWORD}" psql -h "${YB_HOST}" -p "${YB_PORT}" -U "${YB_ADMIN_USER}" -d "${YB_DATABASE}" -f "${SCRIPT_DIR}/create_monitoring_user.sql" > /dev/null 2>&1; then
    log_success "Monitoring user created successfully"
else
    log_error "Failed to create monitoring user"
    exit 1
fi

# Display final summary
echo ""
echo -e "${GREEN}${BOLD}╔════════════════════════════════════════╗"
echo "║     Setup Completed Successfully!      ║"
echo -e "╚════════════════════════════════════════╝${NC}"
echo ""

echo -e "${CYAN}Created Components:${NC}"
echo -e "  ${GREEN}✓${NC} Database: ${BOLD}gv\$${NC}"
echo -e "  ${GREEN}✓${NC} Global Views: ${BOLD}gv\$pg_stat_activity${NC}"
echo -e "  ${GREEN}✓${NC} Global Views: ${BOLD}gv\$pg_stat_statements${NC}"
echo -e "  ${GREEN}✓${NC} Global Views: ${BOLD}gv\$pg_stat_database${NC}"
echo -e "  ${GREEN}✓${NC} History Schema: ${BOLD}gv_history${NC}"
echo -e "  ${GREEN}✓${NC} History Table: ${BOLD}gv_history.global_pg_stat_statements${NC}"
echo -e "  ${GREEN}✓${NC} Monitoring User: ${BOLD}nr_monitor${NC}"
echo ""

echo -e "${YELLOW}${BOLD}╔════════════════════════════════════════╗"
echo "║      Monitoring User Credentials       ║"
echo -e "╚════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${BOLD}Username:${NC} ${CYAN}nr_monitor${NC}"
echo -e "  ${BOLD}Password:${NC} ${CYAN}nr_monitor_2024${NC}"
echo -e "  ${BOLD}Database:${NC} ${CYAN}gv\$${NC}"
echo ""
echo -e "${BOLD}Connection String:${NC}"
echo -e "  ${CYAN}postgresql://nr_monitor:nr_monitor_2024@${YB_HOST}:${YB_PORT}/gv\$${NC}"
echo ""
echo -e "${BOLD}Test Connection:${NC}"
echo -e "  ${CYAN}psql postgresql://nr_monitor:nr_monitor_2024@${YB_HOST}:${YB_PORT}/gv\$${NC}"
echo ""
echo -e "${YELLOW}⚠️  Remember to change the default password in production!${NC}"
echo ""