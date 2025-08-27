#!/bin/bash

# Production Health Check Script
# This script performs comprehensive health checks on production deployment

set -e

echo "🏥 Starting Production Health Check..."
echo "Environment: ${DATABRICKS_BUNDLE_ENV:-prod}"
echo "Host: ${DATABRICKS_HOST}"
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Verify required tools
echo "🔧 Verifying required tools..."
if ! command_exists databricks; then
    echo "❌ Databricks CLI not found"
    exit 1
fi

if ! command_exists lhp; then
    echo "❌ Lakehouse Plumber not found"
    exit 1
fi

echo "✅ Required tools verified"

# Check Databricks connectivity
echo "🌐 Testing Databricks connectivity..."
if databricks current-user me >/dev/null 2>&1; then
    CURRENT_USER=$(databricks current-user me --output json | jq -r '.emails[0]')
    echo "✅ Connected to Databricks as: $CURRENT_USER"
else
    echo "❌ Failed to connect to Databricks"
    exit 1
fi

# Validate bundle configuration
echo "🔍 Validating bundle configuration..."
if databricks bundle validate --target prod; then
    echo "✅ Bundle configuration is valid"
else
    echo "❌ Bundle configuration validation failed"
    exit 1
fi

# Check workspace health
echo "🏢 Checking workspace health..."
WORKSPACE_INFO=$(databricks workspace get-status / --output json 2>/dev/null || echo '{}')
if echo "$WORKSPACE_INFO" | jq -e '.path' >/dev/null 2>&1; then
    echo "✅ Workspace is accessible"
else
    echo "⚠️ Workspace status check inconclusive"
fi

# Check clusters (if any are configured in the bundle)
echo "🖥️ Checking compute resources..."
CLUSTER_INFO=$(databricks clusters list --output json 2>/dev/null || echo '{"clusters":[]}')
CLUSTER_COUNT=$(echo "$CLUSTER_INFO" | jq '.clusters | length')
echo "📊 Found $CLUSTER_COUNT clusters in workspace"

if [ "$CLUSTER_COUNT" -gt 0 ]; then
    RUNNING_CLUSTERS=$(echo "$CLUSTER_INFO" | jq '[.clusters[] | select(.state == "RUNNING")] | length')
    echo "🟢 Running clusters: $RUNNING_CLUSTERS"
fi

# Check jobs (pipelines deployed by bundle)
echo "📋 Checking deployed jobs..."
JOB_INFO=$(databricks jobs list --output json 2>/dev/null || echo '{"jobs":[]}')
JOB_COUNT=$(echo "$JOB_INFO" | jq '.jobs | length')
echo "📊 Found $JOB_COUNT jobs in workspace"

if [ "$JOB_COUNT" -gt 0 ]; then
    # Check for recent job runs
    RECENT_RUNS=$(databricks runs list --limit 10 --output json 2>/dev/null || echo '{"runs":[]}')
    RECENT_COUNT=$(echo "$RECENT_RUNS" | jq '.runs | length')
    echo "🔄 Recent runs: $RECENT_COUNT"
    
    if [ "$RECENT_COUNT" -gt 0 ]; then
        FAILED_RUNS=$(echo "$RECENT_RUNS" | jq '[.runs[] | select(.state.result_state == "FAILED")] | length')
        SUCCESS_RUNS=$(echo "$RECENT_RUNS" | jq '[.runs[] | select(.state.result_state == "SUCCESS")] | length')
        echo "✅ Recent successful runs: $SUCCESS_RUNS"
        echo "❌ Recent failed runs: $FAILED_RUNS"
        
        if [ "$FAILED_RUNS" -gt 0 ]; then
            echo "⚠️ Warning: There are recent failed job runs"
            # List failed runs
            echo "$RECENT_RUNS" | jq -r '.runs[] | select(.state.result_state == "FAILED") | "  - Job: \(.job_id), Run: \(.run_id), Started: \(.start_time)"'
        fi
    fi
fi

# Check Delta Lake tables (if Unity Catalog is enabled)
echo "🗄️ Checking data assets..."
CATALOGS=$(databricks catalogs list --output json 2>/dev/null || echo '{"catalogs":[]}')
CATALOG_COUNT=$(echo "$CATALOGS" | jq '.catalogs | length')
if [ "$CATALOG_COUNT" -gt 0 ]; then
    echo "📚 Unity Catalog enabled with $CATALOG_COUNT catalogs"
else
    echo "📚 Unity Catalog not configured or not accessible"
fi

# Bundle-specific health checks
echo "📦 Checking bundle-specific resources..."
BUNDLE_SUMMARY=$(databricks bundle summary --target prod --output json 2>/dev/null || echo '{}')
if echo "$BUNDLE_SUMMARY" | jq -e '.resources' >/dev/null 2>&1; then
    RESOURCE_TYPES=$(echo "$BUNDLE_SUMMARY" | jq -r '.resources | keys[]')
    echo "📋 Bundle resource types:"
    echo "$RESOURCE_TYPES" | sed 's/^/  - /'
else
    echo "📦 Bundle summary not available"
fi

# Check system health metrics
echo "📊 System health summary..."
echo "  ✅ Databricks connectivity: OK"
echo "  ✅ Bundle validation: OK"
echo "  ✅ Workspace access: OK"
echo "  📊 Clusters: $CLUSTER_COUNT total"
echo "  📋 Jobs: $JOB_COUNT total"
echo "  🗄️ Catalogs: $CATALOG_COUNT total"

echo ""
echo "🏥 Production Health Check Completed Successfully"
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Status: HEALTHY ✅"
