# GitHub Actions CI/CD Setup

## Overview

The Spark integration tests are now configured to run automatically via GitHub Actions.

## Workflow File

**Location**: `.github/workflows/spark-integration-tests.yml`

## Triggers

The workflow runs automatically on:

1. **Push to master/main** - When code is pushed to main branches
2. **Pull Requests** - When PRs target master/main
3. **Manual Trigger** - Via workflow_dispatch in GitHub UI

The workflow only runs when changes are detected in:
- `test/java/spark/**`
- `other/java/hdfs2/**`
- `other/java/hdfs3/**`
- `other/java/client/**`
- The workflow file itself

## Jobs

### Job 1: spark-tests (Required)
**Duration**: ~5-10 minutes

Steps:
1. ✓ Checkout code
2. ✓ Setup JDK 11
3. ✓ Start SeaweedFS (master, volume, filer)
4. ✓ Build project
5. ✓ Run all integration tests (10 tests)
6. ✓ Upload test results
7. ✓ Publish test report
8. ✓ Cleanup

**Test Coverage**:
- SparkReadWriteTest: 6 tests
- SparkSQLTest: 4 tests

### Job 2: spark-example (Optional)
**Duration**: ~5 minutes  
**Runs**: Only on push/manual trigger (not on PRs)

Steps:
1. ✓ Checkout code
2. ✓ Setup JDK 11
3. ✓ Download Apache Spark 3.5.0 (cached)
4. ✓ Start SeaweedFS
5. ✓ Build project
6. ✓ Run example Spark application
7. ✓ Verify output
8. ✓ Cleanup

### Job 3: summary (Status Check)
**Duration**: < 1 minute

Provides overall test status summary.

## Viewing Results

### In GitHub UI

1. Go to the **Actions** tab in your GitHub repository
2. Click on **Spark Integration Tests** workflow
3. View individual workflow runs
4. Check test reports and logs

### Status Badge

Add this badge to your README.md to show the workflow status:

```markdown
[![Spark Integration Tests](https://github.com/seaweedfs/seaweedfs/actions/workflows/spark-integration-tests.yml/badge.svg)](https://github.com/seaweedfs/seaweedfs/actions/workflows/spark-integration-tests.yml)
```

### Test Reports

After each run:
- Test results are uploaded as artifacts (retained for 30 days)
- Detailed JUnit reports are published
- Logs are available for each step

## Configuration

### Environment Variables

Set in the workflow:
```yaml
env:
  SEAWEEDFS_TEST_ENABLED: true
  SEAWEEDFS_FILER_HOST: localhost
  SEAWEEDFS_FILER_PORT: 8888
  SEAWEEDFS_FILER_GRPC_PORT: 18888
```

### Timeout

- spark-tests job: 30 minutes max
- spark-example job: 20 minutes max

## Troubleshooting CI Failures

### SeaweedFS Connection Issues

**Symptom**: Tests fail with connection refused

**Check**:
1. View SeaweedFS logs in the workflow output
2. Look for "Display SeaweedFS logs on failure" step
3. Verify health check succeeded

**Solution**: The workflow already includes retry logic and health checks

### Test Failures

**Symptom**: Tests pass locally but fail in CI

**Check**:
1. Download test artifacts from the workflow run
2. Review detailed surefire reports
3. Check for timing issues or resource constraints

**Common Issues**:
- Docker startup timing (already handled with 30 retries)
- Network issues (retry logic included)
- Resource limits (CI has sufficient memory)

### Build Failures

**Symptom**: Maven build fails

**Check**:
1. Verify dependencies are available
2. Check Maven cache
3. Review build logs

### Example Application Failures

**Note**: This job is optional and only runs on push/manual trigger

**Check**:
1. Verify Spark was downloaded and cached correctly
2. Check spark-submit logs
3. Verify SeaweedFS output directory

## Manual Workflow Trigger

To manually run the workflow:

1. Go to **Actions** tab
2. Select **Spark Integration Tests**
3. Click **Run workflow** button
4. Select branch
5. Click **Run workflow**

This is useful for:
- Testing changes before pushing
- Re-running failed tests
- Testing with different configurations

## Local Testing Matching CI

To run tests locally that match the CI environment:

```bash
# Use the same Docker setup as CI
cd test/java/spark
docker-compose up -d seaweedfs-master seaweedfs-volume seaweedfs-filer

# Wait for services (same as CI)
for i in {1..30}; do
  curl -f http://localhost:8888/ && break
  sleep 2
done

# Run tests (same environment variables as CI)
export SEAWEEDFS_TEST_ENABLED=true
export SEAWEEDFS_FILER_HOST=localhost
export SEAWEEDFS_FILER_PORT=8888
export SEAWEEDFS_FILER_GRPC_PORT=18888
mvn test -B

# Cleanup
docker-compose down -v
```

## Maintenance

### Updating Spark Version

To update to a newer Spark version:

1. Update `pom.xml`: Change `<spark.version>`
2. Update workflow: Change Spark download URL
3. Test locally first
4. Create PR to test in CI

### Updating Java Version

1. Update `pom.xml`: Change `<maven.compiler.source>` and `<target>`
2. Update workflow: Change JDK version in `setup-java` steps
3. Test locally
4. Update README with new requirements

### Adding New Tests

New test classes are automatically discovered and run by the workflow.
Just ensure they:
- Extend `SparkTestBase`
- Use `skipIfTestsDisabled()`
- Are in the correct package

## CI Performance

### Typical Run Times

| Job | Duration | Can Fail Build? |
|-----|----------|-----------------|
| spark-tests | 5-10 min | Yes |
| spark-example | 5 min | No (optional) |
| summary | < 1 min | Only if tests fail |

### Optimizations

The workflow includes:
- ✓ Maven dependency caching
- ✓ Spark binary caching
- ✓ Parallel job execution
- ✓ Smart path filtering
- ✓ Docker layer caching

### Resource Usage

- Memory: ~4GB per job
- Disk: ~2GB (cached)
- Network: ~500MB (first run)

## Security Considerations

- No secrets required (tests use default ports)
- Runs in isolated Docker environment
- Clean up removes all test data
- No external services accessed

## Future Enhancements

Potential improvements:
- [ ] Matrix testing (multiple Spark versions)
- [ ] Performance benchmarking
- [ ] Code coverage reporting
- [ ] Integration with larger datasets
- [ ] Multi-node Spark cluster testing

## Support

If CI tests fail:

1. Check workflow logs in GitHub Actions
2. Download test artifacts for detailed reports
3. Try reproducing locally using the "Local Testing" section above
4. Review recent changes in the failing paths
5. Check SeaweedFS logs in the workflow output

For persistent issues:
- Open an issue with workflow run link
- Include test failure logs
- Note if it passes locally


