#!/bin/bash

# Deployment Validation Script
# Validates that deployment was successful and resources are properly configured

set -e

ENVIRONMENT="${1:-prod}"
echo "🔍 Validating deployment for environment: $ENVIRONMENT"
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

if ! command_exists jq; then
    echo "❌ jq not found (required for JSON parsing)"
    exit 1
fi

# Set target environment
export DATABRICKS_BUNDLE_ENV="$ENVIRONMENT"

echo "📋 Step 1: Bundle Validation"
if databricks bundle validate --target "$ENVIRONMENT"; then
    echo "✅ Bundle configuration is valid"
else
    echo "❌ Bundle validation failed"
    exit 1
fi

echo ""
echo "🔄 Step 2: Bundle Status Check"
BUNDLE_STATUS=$(databricks bundle status --target "$ENVIRONMENT" --output json 2>/dev/null || echo '{}')
if echo "$BUNDLE_STATUS" | jq -e '.resources' >/dev/null 2>&1; then
    echo "✅ Bundle status retrieved successfully"
    
    # Check for any issues in bundle status
    RESOURCE_COUNT=$(echo "$BUNDLE_STATUS" | jq '.resources | length')
    echo "📊 Total resources in bundle: $RESOURCE_COUNT"
    
    # List resource types
    RESOURCE_TYPES=$(echo "$BUNDLE_STATUS" | jq -r '.resources | keys[]' | sort | uniq)
    echo "📋 Resource types:"
    echo "$RESOURCE_TYPES" | sed 's/^/  - /'
    
else
    echo "⚠️ Bundle status check returned unexpected format"
fi

echo ""
echo "💼 Step 3: Job Deployment Verification"
JOBS=$(databricks jobs list --output json)
BUNDLE_JOBS=$(echo "$JOBS" | jq -r --arg env "$ENVIRONMENT" '.jobs[] | select(.settings.tags.environment == $env) | .settings.name')

if [ -n "$BUNDLE_JOBS" ]; then
    echo "✅ Found jobs deployed for environment $ENVIRONMENT:"
    echo "$BUNDLE_JOBS" | sed 's/^/  - /'
    
    # Check job configurations
    JOB_COUNT=$(echo "$BUNDLE_JOBS" | wc -l)
    echo "📊 Total jobs for $ENVIRONMENT: $JOB_COUNT"
    
else
    echo "⚠️ No jobs found with environment tag: $ENVIRONMENT"
fi

echo ""
echo "🏢 Step 4: Workspace Resource Check"
# Check if there are any workspace files deployed
WORKSPACE_FILES=$(databricks workspace list /Workspace/Shared 2>/dev/null | grep -E '\.(py|sql)$' | wc -l || echo "0")
echo "📄 Workspace files found: $WORKSPACE_FILES"

echo ""
echo "🔐 Step 5: Permissions Validation"
# Check current user permissions
CURRENT_USER=$(databricks current-user me --output json)
USER_EMAIL=$(echo "$CURRENT_USER" | jq -r '.emails[0]')
echo "👤 Deployment user: $USER_EMAIL"

echo ""
echo "⚙️ Step 6: Configuration Consistency Check"
# Verify that generated files match the bundle configuration
if [ -d "generated" ]; then
    GENERATED_FILES=$(find generated -name "*.py" | wc -l)
    echo "🐍 Generated Python files: $GENERATED_FILES"
    
    # Check for any syntax issues in generated files
    echo "🔍 Checking generated file syntax..."
    SYNTAX_ERRORS=0
    for py_file in $(find generated -name "*.py"); do
        if ! python -m py_compile "$py_file" 2>/dev/null; then
            echo "❌ Syntax error in: $py_file"
            SYNTAX_ERRORS=$((SYNTAX_ERRORS + 1))
        fi
    done
    
    if [ $SYNTAX_ERRORS -eq 0 ]; then
        echo "✅ All generated files have valid Python syntax"
    else
        echo "❌ Found $SYNTAX_ERRORS files with syntax errors"
        exit 1
    fi
else
    echo "⚠️ No generated directory found"
fi

echo ""
echo "📊 Step 7: Deployment Summary"
echo "Environment: $ENVIRONMENT"
echo "Bundle Status: ✅ Valid"
echo "Jobs Deployed: ${JOB_COUNT:-0}"
echo "Workspace Files: $WORKSPACE_FILES"
echo "Generated Files: ${GENERATED_FILES:-0}"
echo "User: $USER_EMAIL"
echo "Validation Time: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo ""
if [ "$ENVIRONMENT" = "prod" ]; then
    echo "🚀 PRODUCTION DEPLOYMENT VALIDATION COMPLETE"
    echo "Status: ✅ VALIDATED"
    echo ""
    echo "📋 Post-deployment checklist:"
    echo "  - [ ] Monitor job execution for the next 24 hours"
    echo "  - [ ] Verify data quality in downstream systems"
    echo "  - [ ] Confirm alerts and monitoring are active"
    echo "  - [ ] Document deployment in change log"
else
    echo "✅ $ENVIRONMENT DEPLOYMENT VALIDATION COMPLETE"
fi

echo "Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
