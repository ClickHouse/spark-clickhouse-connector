# Test Coverage CI Setup - Quick Reference

## What Was Added

### 1. GitHub Actions Workflow Updates
- **File:** `.github/workflows/build-and-test.yml`
- **Changes:**
  - Added `reportTestScoverage` to test command
  - Added coverage report generation step
  - Added artifact uploads for coverage and test results
  - Added `aggregate-coverage` job to combine all reports
  - Added PR comment with coverage summary

### 2. Python Scripts
- **`generate-coverage-summary.py`** - Creates markdown summaries per build
- **`generate-index-files.py`** - Generates COVERAGE.md index files
- **Location:** `.github/scripts/`

### 3. Generated Files (by CI)
- `COVERAGE.md` - Root coverage index
- `clickhouse-core/COVERAGE.md` - Core module index
- `spark-{version}/COVERAGE.md` - Per-Spark-version index
- `coverage-reports/*.md` - Per-configuration reports
- `coverage-reports/*.json` - Metadata for each build

## How It Works

### Build Phase (Matrix Job)
For each Java/Spark/Scala combination:
```
1. Run tests with coverage
   └─> ./gradlew test reportTestScoverage
2. Generate summary markdown
   └─> generate-coverage-summary.py
3. Upload artifacts
   ├─> coverage-java{X}-spark{Y}-scala{Z}
   └─> test-results-java{X}-spark{Y}-scala{Z}
```

### Aggregation Phase
After all builds complete:
```
1. Download all coverage artifacts
2. Organize reports by module
3. Generate index files
   └─> COVERAGE.md in root and each module
4. Upload aggregated artifact
5. Post PR comment (if PR)
```

## Directory Structure

```
project-root/
├── .github/
│   ├── workflows/
│   │   └── build-and-test.yml          # Updated workflow
│   └── scripts/
│       ├── generate-coverage-summary.py
│       ├── generate-index-files.py
│       └── README.md
├── COVERAGE.md                          # Auto-generated index
├── COVERAGE.md.template                 # Template for reference
├── coverage-reports/                    # Generated summaries (gitignored)
│   ├── coverage-java8-spark3.5-scala2.12.md
│   ├── coverage-java8-spark3.5-scala2.12.json
│   └── ...
├── clickhouse-core/
│   ├── COVERAGE.md                      # Auto-generated
│   └── build/reports/scoverageTest/     # HTML reports
└── spark-{version}/
    ├── COVERAGE.md                      # Auto-generated
    └── */build/reports/scoverageTest/   # HTML reports
```

## Viewing Coverage Reports

### On GitHub (PR/Push)
1. Go to Actions tab → Select workflow run
2. Scroll to "Artifacts" section
3. Download `coverage-aggregated`
4. Extract and open `COVERAGE.md` or any `index.html`

### Locally
```bash
# Generate coverage for current configuration
./gradlew test reportTestScoverage

# View in browser
open build/reports/scoverage/index.html
open clickhouse-core/build/reports/scoverageTest/index.html
open spark-3.5/clickhouse-spark/build/reports/scoverageTest/index.html
```

### Generate Full Report Locally
```bash
# Run for all configurations (example for Spark 3.5)
for java in 8 17; do
  for scala in 2.12 2.13; do
    echo "Building Java $java, Spark 3.5, Scala $scala"
    ./gradlew clean test reportTestScoverage \
      -Dspark_binary_version=3.5 \
      -Dscala_binary_version=$scala
    
    python3 .github/scripts/generate-coverage-summary.py \
      --spark-version 3.5 \
      --scala-version $scala \
      --java-version $java \
      --output-dir coverage-reports
  done
done

# Generate index files
python3 .github/scripts/generate-index-files.py

# View root index
cat COVERAGE.md
```

## Artifacts Generated

| Artifact Name | Contents | When |
|--------------|----------|------|
| `coverage-java{X}-spark{Y}-scala{Z}` | Coverage reports for specific config | Per build |
| `test-results-java{X}-spark{Y}-scala{Z}` | Test results for specific config | Per build |
| `coverage-aggregated` | All coverage + index files | After all builds |
| `log-java{X}-spark{Y}-scala{Z}` | Build logs | On failure |

## PR Comments

When a PR is created/updated, the workflow will post a comment with:
- Coverage summary table
- Links to detailed reports
- Comparison with base branch (if available)

## Customization

### Change Coverage Thresholds
Edit `build.gradle`:
```gradle
scoverage {
    minimumRate.set(0.75)  // 75% minimum coverage
}
```

### Add Coverage Badge
Add to README.md:
```markdown
![Coverage](https://img.shields.io/badge/coverage-XX%25-brightgreen)
```

### Integrate with External Services
Add to workflow:
```yaml
- name: Upload to Codecov
  uses: codecov/codecov-action@v3
  with:
    files: ./build/reports/scoverage/cobertura.xml
```

## Troubleshooting

### Coverage reports not generated
- Check that `reportTestScoverage` task runs successfully
- Verify scoverage plugin is configured in `build.gradle`
- Check workflow logs for errors

### Index files not created
- Ensure `coverage-reports/` directory has JSON files
- Check Python script execution in workflow logs
- Verify file paths match expected structure

### Artifacts not uploaded
- Check artifact size (GitHub has limits)
- Verify paths in workflow match actual file locations
- Ensure `if: always()` is set for upload steps

## Next Steps

1. **Monitor Coverage Trends**
   - Track coverage over time
   - Set up alerts for coverage drops

2. **Add Coverage Goals**
   - Define target coverage per module
   - Fail builds if coverage drops below threshold

3. **Integrate with Code Review**
   - Require coverage for new code
   - Show coverage diff in PRs

4. **Optimize CI**
   - Cache dependencies
   - Parallelize test execution
   - Skip unchanged modules
