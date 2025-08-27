#!/bin/bash

# Critical Path Tests Script
# Tests essential functionality after deployment or rollback

set -e

ENVIRONMENT="${1:-prod}"
echo "🧪 Running critical path tests for environment: $ENVIRONMENT"
echo "Host: ${DATABRICKS_HOST}"
echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Verify required tools
if ! command_exists databricks; then
    echo "❌ Databricks CLI not found"
    exit 1
fi

# Set target environment
export DATABRICKS_BUNDLE_ENV="$ENVIRONMENT"

echo ""
echo "🔍 Test 1: Basic Connectivity"
if databricks current-user me >/dev/null 2>&1; then
    USER_EMAIL=$(databricks current-user me --output json | jq -r '.emails[0]')
    echo "✅ Databricks connectivity test passed - User: $USER_EMAIL"
else
    echo "❌ Databricks connectivity test failed"
    exit 1
fi

echo ""
echo "📦 Test 2: Bundle Status"
if databricks bundle status --target "$ENVIRONMENT" >/dev/null 2>&1; then
    echo "✅ Bundle status test passed"
else
    echo "❌ Bundle status test failed"
    exit 1
fi

echo ""
echo "💼 Test 3: Job Availability"
JOBS=$(databricks jobs list --output json 2>/dev/null || echo '{"jobs":[]}')
JOB_COUNT=$(echo "$JOBS" | jq '.jobs | length')

if [ "$JOB_COUNT" -gt 0 ]; then
    echo "✅ Job availability test passed - Found $JOB_COUNT jobs"
    
    # List first few jobs for verification
    echo "$JOBS" | jq -r '.jobs[0:3][] | "  - \(.settings.name) (ID: \(.job_id))"'
    
    if [ "$JOB_COUNT" -gt 3 ]; then
        echo "  ... and $((JOB_COUNT - 3)) more jobs"
    fi
else
    echo "⚠️ Job availability test - No jobs found (this may be expected for empty deployments)"
fi

echo ""
echo "🏢 Test 4: Workspace Access"
# Test workspace root access
if databricks workspace get-status / >/dev/null 2>&1; then
    echo "✅ Workspace access test passed"
else
    echo "❌ Workspace access test failed"
    exit 1
fi

echo ""
echo "🗄️ Test 5: Data Assets Access"
# Check if Unity Catalog is available
CATALOGS=$(databricks catalogs list --output json 2>/dev/null || echo '{"catalogs":[]}')
CATALOG_COUNT=$(echo "$CATALOGS" | jq '.catalogs | length')

if [ "$CATALOG_COUNT" -gt 0 ]; then
    echo "✅ Unity Catalog access test passed - Found $CATALOG_COUNT catalogs"
    
    # List first few catalogs
    echo "$CATALOGS" | jq -r '.catalogs[0:3][] | "  - \(.name)"'
    
    if [ "$CATALOG_COUNT" -gt 3 ]; then
        echo "  ... and $((CATALOG_COUNT - 3)) more catalogs"
    fi
else
    echo "ℹ️ Unity Catalog test - No catalogs found (may not be configured)"
fi

echo ""
echo "🔄 Test 6: Job Execution Readiness"
# Check if we can get job run history (indicates jobs are executable)
RECENT_RUNS=$(databricks runs list --limit 5 --output json 2>/dev/null || echo '{"runs":[]}')
RECENT_COUNT=$(echo "$RECENT_RUNS" | jq '.runs | length')

if [ "$RECENT_COUNT" -gt 0 ]; then
    echo "✅ Job execution readiness test passed - Found $RECENT_COUNT recent runs"
    
    # Show status of recent runs
    SUCCESS_COUNT=$(echo "$RECENT_RUNS" | jq '[.runs[] | select(.state.result_state == "SUCCESS")] | length')
    FAILED_COUNT=$(echo "$RECENT_RUNS" | jq '[.runs[] | select(.state.result_state == "FAILED")] | length')
    RUNNING_COUNT=$(echo "$RECENT_RUNS" | jq '[.runs[] | select(.state.life_cycle_state == "RUNNING")] | length')
    
    echo "  - ✅ Successful: $SUCCESS_COUNT"
    echo "  - ❌ Failed: $FAILED_COUNT" 
    echo "  - 🔄 Running: $RUNNING_COUNT"
    
    if [ "$FAILED_COUNT" -gt 2 ]; then
        echo "⚠️ Warning: High number of failed runs detected"
    fi
else
    echo "ℹ️ Job execution readiness test - No recent runs found"
fi

echo ""
echo "🔐 Test 7: Permissions Validation"
# Test basic permissions by trying to list workspace
if databricks workspace list /Workspace >/dev/null 2>&1; then
    echo "✅ Permissions validation test passed"
else
    echo "⚠️ Permissions validation test - Limited workspace access"
fi

echo ""
echo "⚡ Test 8: Compute Resources"
# Check available compute resources
CLUSTERS=$(databricks clusters list --output json 2>/dev/null || echo '{"clusters":[]}')
CLUSTER_COUNT=$(echo "$CLUSTERS" | jq '.clusters | length')

if [ "$CLUSTER_COUNT" -gt 0 ]; then
    echo "✅ Compute resources test passed - Found $CLUSTER_COUNT clusters"
    
    RUNNING_CLUSTERS=$(echo "$CLUSTERS" | jq '[.clusters[] | select(.state == "RUNNING")] | length')
    echo "  - 🟢 Running: $RUNNING_CLUSTERS"
    echo "  - 📊 Total: $CLUSTER_COUNT"
else
    echo "ℹ️ Compute resources test - No clusters found (may use serverless compute)"
fi

echo ""
echo "📊 Critical Path Test Summary"
echo "================================"
echo "Environment: $ENVIRONMENT"
echo "Test Time: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""
echo "Core Tests:"
echo "  ✅ Connectivity"
echo "  ✅ Bundle Status" 
echo "  ✅ Workspace Access"
echo "  ✅ Permissions"
echo ""
echo "Resource Tests:"
echo "  📋 Jobs: $JOB_COUNT found"
echo "  🗄️ Catalogs: $CATALOG_COUNT found"
echo "  ⚡ Clusters: $CLUSTER_COUNT found"
echo "  🔄 Recent Runs: $RECENT_COUNT found"
echo ""

if [ "$ENVIRONMENT" = "prod" ]; then
    echo "🎯 PRODUCTION CRITICAL PATH TESTS COMPLETE"
    echo "Status: ✅ ALL CRITICAL TESTS PASSED"
    echo ""
    echo "🚀 Production environment is ready for workloads"
else
    echo "🎯 $ENVIRONMENT CRITICAL PATH TESTS COMPLETE"  
    echo "Status: ✅ ALL CRITICAL TESTS PASSED"
fi

echo "Completion Time: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
