# GitHub Pages Setup for Coverage Reports

## What This Does

The CI workflow now automatically publishes coverage reports to GitHub Pages, giving you a permanent URL where you can view all test coverage for every Java/Spark/Scala combination.

## Enable GitHub Pages

### For Your Fork (ShimonSte/spark-clickhouse-connector)

1. **Go to your repository settings:**
   - Navigate to: https://github.com/ShimonSte/spark-clickhouse-connector/settings/pages

2. **Configure GitHub Pages:**
   - **Source:** Deploy from a branch
   - **Branch:** `gh-pages`
   - **Folder:** `/ (root)`
   - Click **Save**

3. **Wait for deployment:**
   - First deployment takes 1-2 minutes
   - GitHub will show the URL where your site is published

4. **Access your coverage reports:**
   - URL will be: `https://shimonste.github.io/spark-clickhouse-connector/`
   - This updates automatically on every push to `main`

### For Main ClickHouse Repository

If this gets merged to the main repository, the ClickHouse maintainers will need to:

1. Enable GitHub Pages in repository settings
2. Set source to `gh-pages` branch
3. Coverage will be available at: `https://clickhouse.github.io/spark-clickhouse-connector/`

## How It Works

```
Push to main
    ↓
CI runs tests with coverage
    ↓
Generates HTML reports
    ↓
Creates index.html with links
    ↓
Deploys to gh-pages branch
    ↓
GitHub Pages serves the site
    ↓
Coverage reports accessible via URL
```

## What You'll See

### Landing Page
- Clean, organized index of all modules
- Links to coverage reports for each module
- Last updated timestamp

### Module Reports
- Interactive HTML coverage reports
- Line-by-line coverage visualization
- Coverage percentages for all files
- Drill-down into specific files

## Structure

```
https://shimonste.github.io/spark-clickhouse-connector/
├── index.html                                          # Main landing page
├── clickhouse-core/
│   ├── COVERAGE.md
│   └── build/reports/scoverageTest/index.html         # Core coverage
├── clickhouse-core-it/
│   └── build/reports/scoverageTest/index.html         # Core IT coverage
├── spark-3.3/
│   ├── COVERAGE.md
│   ├── clickhouse-spark/build/reports/scoverageTest/
│   └── clickhouse-spark-it/build/reports/scoverageTest/
├── spark-3.4/
│   └── ... (same structure)
└── spark-3.5/
    └── ... (same structure)
```

## Benefits

✅ **Always Accessible** - Permanent URL, no need to download artifacts
✅ **Auto-Updated** - Refreshes on every push to main
✅ **All Configurations** - Coverage for all Java/Spark/Scala combinations
✅ **Interactive** - Full HTML reports with drill-down
✅ **Shareable** - Easy to link in issues, PRs, documentation
✅ **No Git Clutter** - Reports stored in separate `gh-pages` branch

## Linking to Coverage

Once enabled, you can:

### In README.md
```markdown
[![Coverage](https://img.shields.io/badge/coverage-view%20reports-blue)](https://shimonste.github.io/spark-clickhouse-connector/)
```

### In Issues/PRs
```markdown
See [test coverage reports](https://shimonste.github.io/spark-clickhouse-connector/)
```

### Direct Links
```markdown
- [Spark 3.5 Coverage](https://shimonste.github.io/spark-clickhouse-connector/spark-3.5/clickhouse-spark/build/reports/scoverageTest/index.html)
- [Core Module Coverage](https://shimonste.github.io/spark-clickhouse-connector/clickhouse-core/build/reports/scoverageTest/index.html)
```

## Troubleshooting

### Pages Not Showing Up
- Check that GitHub Pages is enabled in settings
- Verify `gh-pages` branch exists after CI runs
- Wait 1-2 minutes for first deployment

### 404 Errors
- Ensure the workflow ran successfully on `main` branch
- Check that files were copied to `gh-pages` branch
- Verify paths in index.html match actual file structure

### Reports Not Updating
- GitHub Pages deployment only runs on pushes to `main`
- PRs and other branches don't trigger deployment
- Check workflow logs for deployment step

## Security Note

The `peaceiris/actions-gh-pages@v3` action uses `GITHUB_TOKEN` which is automatically provided by GitHub Actions. No additional secrets needed.

## Next Steps

1. **Enable GitHub Pages** in your fork's settings
2. **Push this change** to trigger the first deployment
3. **Visit the URL** to see your coverage reports
4. **Share the link** with your team

---

**Ready to enable?** Go to: https://github.com/ShimonSte/spark-clickhouse-connector/settings/pages
