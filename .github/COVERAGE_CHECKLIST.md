# Coverage CI Setup Checklist

Use this checklist to verify the coverage CI setup is working correctly.

## ✅ Pre-Commit Checklist

- [ ] Review modified workflow file: `.github/workflows/build-and-test.yml`
- [ ] Verify Python scripts are executable:
  ```bash
  chmod +x .github/scripts/*.py
  ```
- [ ] Test scripts locally (optional):
  ```bash
  ./gradlew test reportTestScoverage -Dspark_binary_version=3.5 -Dscala_binary_version=2.12
  python3 .github/scripts/generate-coverage-summary.py --spark-version 3.5 --scala-version 2.12 --java-version 8 --output-dir coverage-reports
  ```
- [ ] Review `.gitignore` changes
- [ ] Read `COVERAGE_CI_SUMMARY.md` for overview

## ✅ Post-Commit Checklist

### 1. Verify Workflow Runs
- [ ] Go to **Actions** tab in GitHub
- [ ] Find the workflow run for your commit
- [ ] Check that all matrix jobs complete successfully
- [ ] Verify `aggregate-coverage` job runs after matrix jobs

### 2. Check Artifacts
- [ ] Scroll to **Artifacts** section in workflow run
- [ ] Verify presence of artifacts:
  - [ ] `coverage-java8-spark3.3-scala2.12`
  - [ ] `coverage-java8-spark3.3-scala2.13`
  - [ ] `coverage-java8-spark3.4-scala2.12`
  - [ ] `coverage-java8-spark3.4-scala2.13`
  - [ ] `coverage-java8-spark3.5-scala2.12`
  - [ ] `coverage-java8-spark3.5-scala2.13`
  - [ ] `coverage-java17-spark3.3-scala2.12`
  - [ ] `coverage-java17-spark3.3-scala2.13`
  - [ ] `coverage-java17-spark3.4-scala2.12`
  - [ ] `coverage-java17-spark3.4-scala2.13`
  - [ ] `coverage-java17-spark3.5-scala2.12`
  - [ ] `coverage-java17-spark3.5-scala2.13`
  - [ ] `coverage-aggregated` ⭐ (Most important)

### 3. Verify Coverage Reports
- [ ] Download `coverage-aggregated` artifact
- [ ] Extract the zip file
- [ ] Verify file structure:
  ```
  coverage-aggregated/
  ├── COVERAGE.md
  ├── coverage-reports/
  │   ├── *.md files
  │   └── *.json files
  ├── clickhouse-core/
  │   └── COVERAGE.md
  └── spark-*/
      └── COVERAGE.md
  ```
- [ ] Open `COVERAGE.md` and verify:
  - [ ] Contains links to module coverage
  - [ ] Shows coverage statistics
  - [ ] Links are properly formatted

### 4. Check Module Reports
- [ ] Open `clickhouse-core/COVERAGE.md`
  - [ ] Contains table with all configurations
  - [ ] Shows statement and branch coverage
  - [ ] Has links to HTML reports
- [ ] Open `spark-3.5/COVERAGE.md`
  - [ ] Contains tables for each module
  - [ ] Shows coverage for all Java/Scala combinations
  - [ ] Has links to HTML reports

### 5. Verify HTML Reports
- [ ] Navigate to HTML report directories
- [ ] Open `index.html` files in browser
- [ ] Verify interactive coverage reports load correctly
- [ ] Check that you can:
  - [ ] View coverage by file
  - [ ] See line-by-line coverage
  - [ ] Navigate through packages

### 6. Test PR Integration (if applicable)
- [ ] Create a test PR
- [ ] Wait for workflow to complete
- [ ] Check for PR comment with coverage summary
- [ ] Verify comment contains:
  - [ ] Coverage statistics
  - [ ] Link to artifacts
  - [ ] Properly formatted markdown

## ✅ Troubleshooting Checklist

If something doesn't work:

### Workflow Fails
- [ ] Check workflow logs in Actions tab
- [ ] Look for error messages in failed steps
- [ ] Verify Gradle command syntax
- [ ] Check Python script errors

### No Coverage Reports
- [ ] Verify `reportTestScoverage` task runs
- [ ] Check that scoverage plugin is configured
- [ ] Look for `build/reports/scoverageTest/` directories
- [ ] Verify XML files are generated

### Missing Artifacts
- [ ] Check artifact upload step logs
- [ ] Verify file paths in workflow match actual locations
- [ ] Check artifact size (GitHub has limits)
- [ ] Ensure `if: always()` is set for upload steps

### Index Files Not Generated
- [ ] Check `aggregate-coverage` job logs
- [ ] Verify Python script execution
- [ ] Check that JSON files exist in `coverage-reports/`
- [ ] Verify file paths match expected structure

### PR Comment Not Posted
- [ ] Check if workflow has write permissions
- [ ] Verify `github-script` action runs
- [ ] Check for errors in script execution
- [ ] Ensure `COVERAGE.md` file exists

## ✅ Validation Tests

Run these tests to ensure everything works:

### Test 1: Local Coverage Generation
```bash
./gradlew clean test reportTestScoverage \
  -Dspark_binary_version=3.5 \
  -Dscala_binary_version=2.12

# Should create:
# - build/reports/scoverage/index.html
# - clickhouse-core/build/reports/scoverageTest/index.html
# - spark-3.5/clickhouse-spark/build/reports/scoverageTest/index.html
```

### Test 2: Summary Generation
```bash
python3 .github/scripts/generate-coverage-summary.py \
  --spark-version 3.5 \
  --scala-version 2.12 \
  --java-version 8 \
  --output-dir coverage-reports

# Should create:
# - coverage-reports/coverage-java8-spark3.5-scala2.12.md
# - coverage-reports/coverage-java8-spark3.5-scala2.12.json
```

### Test 3: Index Generation
```bash
# After running multiple configurations
python3 .github/scripts/generate-index-files.py

# Should create:
# - COVERAGE.md
# - clickhouse-core/COVERAGE.md
# - spark-*/COVERAGE.md
```

## ✅ Success Criteria

Your setup is successful when:

- [ ] All workflow jobs complete without errors
- [ ] Coverage artifacts are uploaded for all configurations
- [ ] `coverage-aggregated` artifact contains all expected files
- [ ] COVERAGE.md files are properly generated
- [ ] HTML reports are viewable and interactive
- [ ] PR comments appear on pull requests (if applicable)
- [ ] Coverage statistics are accurate and match test runs

## 📝 Notes

- First run may take longer as dependencies are downloaded
- Coverage percentages may be low initially - that's expected
- You can run specific configurations locally for faster testing
- Check `.github/COVERAGE_SETUP.md` for detailed documentation

## 🎉 All Done?

If all items are checked, your coverage CI is fully operational!

Next steps:
1. Share `COVERAGE_CI_SUMMARY.md` with your team
2. Set coverage goals for each module
3. Add coverage badges to README
4. Monitor coverage trends over time
