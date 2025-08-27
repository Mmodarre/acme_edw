# GitHub Actions CI/CD Setup Guide

This guide provides step-by-step instructions to configure your GitHub repository for Lakehouse Plumber CI/CD workflows based on the [Databricks GitHub Actions documentation](https://docs.databricks.com/aws/en/dev-tools/ci-cd/github).

## 🔧 Prerequisites

1. **Databricks Workspaces**: Three separate workspaces for dev, test, and production
2. **Databricks Tokens**: Service principal tokens for each environment
3. **GitHub Repository**: Admin access to configure environments and secrets
4. **Lakehouse Plumber**: Project configured with `lhp.yaml` and environment substitutions

## 🏢 GitHub Environment Configuration

### 1. Create GitHub Environments

In your GitHub repository, go to **Settings** → **Environments** and create the following environments:

#### Development Environment
- **Name**: `development`
- **Protection Rules**: None (automatic deployment)
- **Environment Secrets**: See secrets section below

#### Test Environment  
- **Name**: `test`
- **Protection Rules**: None (tag-triggered deployment)
- **Environment Secrets**: See secrets section below

#### Production Environment
- **Name**: `production`
- **Protection Rules**: 
  - ✅ **Required reviewers**: Add `Mmodarre` 
  - ✅ **Wait timer**: 0 minutes (immediate after approval)
  - ✅ **Prevent administrators from bypassing**: Recommended
- **Environment URL**: `https://your-prod-databricks-workspace.cloud.databricks.com`
- **Environment Secrets**: See secrets section below

#### Production Emergency Environment (for rollbacks)
- **Name**: `production-emergency`  
- **Protection Rules**:
  - ✅ **Required reviewers**: Add `Mmodarre`
  - ⚠️ **Wait timer**: 0 minutes (emergency rollback)
- **Environment Secrets**: Same as production

#### Production Maintenance Environment (for planned rollbacks)
- **Name**: `production-maintenance`
- **Protection Rules**:
  - ✅ **Required reviewers**: Add `Mmodarre`
  - ✅ **Wait timer**: 5 minutes (planned rollback)
- **Environment Secrets**: Same as production

## 🔐 GitHub Secrets Configuration

### Repository Secrets
Go to **Settings** → **Secrets and variables** → **Actions** and add:

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `DATABRICKS_DEV_TOKEN` | Development workspace service principal token | `dapi1234567890abcdef...` |
| `DATABRICKS_DEV_HOST` | Development workspace URL | `https://dev-workspace.cloud.databricks.com` |
| `DATABRICKS_TEST_TOKEN` | Test workspace service principal token | `dapi1234567890abcdef...` |
| `DATABRICKS_TEST_HOST` | Test workspace URL | `https://test-workspace.cloud.databricks.com` |
| `DATABRICKS_PROD_TOKEN` | Production workspace service principal token | `dapi1234567890abcdef...` |
| `DATABRICKS_PROD_HOST` | Production workspace URL | `https://prod-workspace.cloud.databricks.com` |

### Environment Secrets
Each environment should have access to its respective secrets:

- **development**: `DATABRICKS_DEV_TOKEN`, `DATABRICKS_DEV_HOST`
- **test**: `DATABRICKS_TEST_TOKEN`, `DATABRICKS_TEST_HOST`  
- **production**: `DATABRICKS_PROD_TOKEN`, `DATABRICKS_PROD_HOST`
- **production-emergency**: `DATABRICKS_PROD_TOKEN`, `DATABRICKS_PROD_HOST`
- **production-maintenance**: `DATABRICKS_PROD_TOKEN`, `DATABRICKS_PROD_HOST`

## 🤖 Service Principal Setup

For each Databricks workspace, create a service principal with appropriate permissions:

### 1. Create Service Principal
```bash
# Using Databricks CLI (for each workspace)
databricks service-principals create --display-name "GitHub-Actions-CI-CD"
```

### 2. Generate Token
```bash
# Generate token for the service principal
databricks tokens create --comment "GitHub Actions CI/CD" --lifetime-seconds 0
```

### 3. Assign Permissions
The service principal needs:
- **Workspace access**: Can access workspace and manage resources
- **Cluster access**: Can create and manage clusters (if using interactive clusters)
- **Job management**: Can create, update, and run jobs
- **Unity Catalog**: Appropriate catalog/schema permissions (if using UC)

## 🏷️ Tag-Based Promotion Workflow

### Development Deployment (Automatic)
- **Trigger**: Push to `main` branch
- **Action**: Automatically deploys to development environment
- **Command**: `git push origin main`

### Test Deployment (Developer Self-Service)
```bash
# Create test deployment tag
git tag v1.2.3-test
git push origin v1.2.3-test
```

### Production Deployment (Approval Required)
```bash
# Create production deployment tag (requires approval)
git tag v1.2.3-prod  
git push origin v1.2.3-prod
```

### Emergency Rollback (Approval Required)
```bash
# Create emergency rollback tag (requires approval)
git tag v1.2.1-rollback  # Roll back to v1.2.1
git push origin v1.2.1-rollback
```

### Planned Maintenance Rollback (Approval Required)
```bash
# Create maintenance rollback tag (requires approval)
git tag v1.2.1-maintenance  # Roll back to v1.2.1
git push origin v1.2.1-maintenance
```

## 🔒 Branch Protection Rules

Configure branch protection for `main` branch:

Go to **Settings** → **Branches** → **Add rule**:
- **Branch name pattern**: `main`
- ✅ **Require pull request reviews before merging**
- ✅ **Require status checks to pass before merging**
  - Add status check: `validate`
- ✅ **Require branches to be up to date before merging**  
- ✅ **Require linear history** (recommended)
- ✅ **Include administrators** (recommended)

## 📋 Workflow Files Checklist

Ensure these files are in your repository:

- [ ] `.github/workflows/lakehouse-cicd.yml` - Main CI/CD pipeline
- [ ] `.github/workflows/rollback.yml` - Emergency rollback procedures  
- [ ] `.github/workflows/drift-detection.yml` - Daily configuration monitoring
- [ ] `scripts/production-health-check.sh` - Production health validation
- [ ] `scripts/validate-deployment.sh` - Deployment validation
- [ ] `scripts/critical-path-tests.sh` - Critical functionality tests
- [ ] `lhp.yaml` - Lakehouse Plumber configuration
- [ ] `substitutions/dev.yaml` - Development environment config
- [ ] `substitutions/tst.yaml` - Test environment config  
- [ ] `substitutions/prod.yaml` - Production environment config
- [ ] `databricks.yml` - Databricks bundle configuration

## 🚀 Testing the Setup

### 1. Test PR Validation
```bash
# Create a feature branch and PR to test validation
git checkout -b test-ci-setup
echo "# Test change" >> README.md
git commit -am "Test CI setup"
git push origin test-ci-setup
# Create PR to main - should trigger validation workflow
```

### 2. Test Development Deployment  
```bash
# Merge PR to main - should trigger dev deployment
git checkout main
git merge test-ci-setup
git push origin main
```

### 3. Test Tag-Based Promotion
```bash
# Test promotion to test environment
git tag v0.1.0-test
git push origin v0.1.0-test

# Test production deployment (requires approval)
git tag v0.1.0-prod
git push origin v0.1.0-prod
```

## 🔍 Monitoring and Troubleshooting

### GitHub Actions Logs
- Navigate to **Actions** tab in your repository
- Click on workflow runs to view detailed logs
- Check job-specific logs for troubleshooting

### Databricks Bundle Logs
```bash
# Check bundle validation locally
databricks bundle validate --target dev

# Check bundle status
databricks bundle status --target prod
```

### Common Issues

1. **Authentication Failures**
   - Verify token validity and permissions
   - Check workspace URLs are correct
   - Ensure service principal has required access

2. **Bundle Validation Errors**
   - Run `lhp validate --env prod` locally
   - Check YAML syntax in pipeline configurations
   - Verify substitution files have required values

3. **Environment Access Issues**  
   - Confirm GitHub environment configurations
   - Verify required reviewers are added
   - Check environment protection rules

## 📖 Additional Resources

- [Lakehouse Plumber CI/CD Documentation](file:///Users/mehdi.modarressi/Documents/Coding/Lakehouse_Plumber/docs/_build/html/cicd_reference.html)
- [Databricks GitHub Actions Guide](https://docs.databricks.com/aws/en/dev-tools/ci-cd/github)
- [GitHub Environments Documentation](https://docs.github.com/en/actions/deployment/targeting-different-environments/using-environments-for-deployment)
- [GitHub Secrets Documentation](https://docs.github.com/en/actions/security-guides/encrypted-secrets)

## 🆘 Support

If you encounter issues:
1. Check GitHub Actions logs for specific error messages
2. Validate configurations locally using LHP and Databricks CLI
3. Verify service principal permissions in each workspace
4. Review branch protection and environment settings

## 🔄 Next Steps After Setup

1. **Monitor** the first few deployments closely
2. **Customize** health checks and validation scripts for your specific pipelines
3. **Document** any custom deployment procedures for your team
4. **Train** team members on the tag-based promotion workflow
5. **Establish** incident response procedures for production issues
