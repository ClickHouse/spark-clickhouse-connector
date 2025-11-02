#!/bin/bash
set -e

echo "🚀 Simulating CI Coverage Report Generation"
echo "==========================================="
echo ""

# Configuration - you can change these
SPARK_VERSION="3.5"
SCALA_VERSION="2.12"
JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)

echo "📋 Configuration:"
echo "  - Java: $JAVA_VERSION"
echo "  - Spark: $SPARK_VERSION"
echo "  - Scala: $SCALA_VERSION"
echo ""

# Step 1: Run tests with coverage
echo "🧪 Step 1: Running tests with coverage..."
./gradlew clean test reportTestScoverage \
  -Dspark_binary_version=$SPARK_VERSION \
  -Dscala_binary_version=$SCALA_VERSION \
  --no-daemon

echo ""
echo "✅ Tests completed!"
echo ""

# Step 2: Create COVERAGE.md files
echo "📝 Step 2: Creating COVERAGE.md index files..."

# Create root COVERAGE.md
cat > COVERAGE.md << 'EOF'
# Test Coverage Reports

Coverage reports are generated for all Java/Spark/Scala combinations.

## Module Coverage

- [ClickHouse Core](clickhouse-core/COVERAGE.md)
- [Spark 3.3](spark-3.3/COVERAGE.md)
- [Spark 3.4](spark-3.4/COVERAGE.md)
- [Spark 3.5](spark-3.5/COVERAGE.md)

## Viewing Reports

Download the `coverage-aggregated` artifact from the workflow run and open the HTML reports:
- `clickhouse-core/build/reports/scoverageTest/index.html`
- `spark-{version}/clickhouse-spark/build/reports/scoverageTest/index.html`
EOF

# Create clickhouse-core/COVERAGE.md
mkdir -p clickhouse-core
cat > clickhouse-core/COVERAGE.md << 'EOF'
# ClickHouse Core - Test Coverage

## Coverage Reports

- [clickhouse-core HTML Report](build/reports/scoverageTest/index.html)
- [clickhouse-core-it HTML Report](../clickhouse-core-it/build/reports/scoverageTest/index.html)

Reports are available for all Java/Spark/Scala combinations in the workflow artifacts.
EOF

# Create spark-*/COVERAGE.md for each version
for spark_ver in 3.3 3.4 3.5; do
  mkdir -p "spark-${spark_ver}"
  cat > "spark-${spark_ver}/COVERAGE.md" << EOF
# Spark ${spark_ver} - Test Coverage

## Coverage Reports

- [clickhouse-spark HTML Report](clickhouse-spark/build/reports/scoverageTest/index.html)
- [clickhouse-spark-it HTML Report](clickhouse-spark-it/build/reports/scoverageTest/index.html)

Reports are available for all Java/Scala combinations in the workflow artifacts.
EOF
done

echo "✅ COVERAGE.md files created!"
echo ""

# Step 3: Create GitHub Pages structure
echo "🌐 Step 3: Creating GitHub Pages structure..."

rm -rf gh-pages
mkdir -p gh-pages

# Copy all coverage reports to gh-pages
cp -r COVERAGE.md gh-pages/index.md
[ -d clickhouse-core ] && cp -r clickhouse-core gh-pages/
[ -d clickhouse-core-it ] && cp -r clickhouse-core-it gh-pages/
[ -d spark-3.3 ] && cp -r spark-3.3 gh-pages/ 2>/dev/null || true
[ -d spark-3.4 ] && cp -r spark-3.4 gh-pages/ 2>/dev/null || true
[ -d spark-3.5 ] && cp -r spark-3.5 gh-pages/ 2>/dev/null || true

# Create a simple HTML index
cat > gh-pages/index.html << 'EOF'
<!DOCTYPE html>
<html>
<head>
  <title>Test Coverage Reports - Spark ClickHouse Connector</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; max-width: 1200px; margin: 40px auto; padding: 0 20px; }
    h1 { color: #333; border-bottom: 2px solid #e1e4e8; padding-bottom: 10px; }
    h2 { color: #0366d6; margin-top: 30px; }
    .module-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; margin: 20px 0; }
    .module-card { border: 1px solid #e1e4e8; border-radius: 6px; padding: 20px; background: #f6f8fa; }
    .module-card h3 { margin-top: 0; color: #0366d6; }
    .module-card a { color: #0366d6; text-decoration: none; display: block; margin: 8px 0; }
    .module-card a:hover { text-decoration: underline; }
    .info { background: #fff3cd; border: 1px solid #ffc107; border-radius: 4px; padding: 15px; margin: 20px 0; }
    .timestamp { color: #586069; font-size: 14px; margin-top: 30px; }
  </style>
</head>
<body>
  <h1>📊 Test Coverage Reports</h1>
  <div class="info">
    <strong>Last Updated:</strong> $(date -u '+%Y-%m-%d %H:%M:%S UTC')
  </div>
  
  <h2>Core Modules</h2>
  <div class="module-grid">
    <div class="module-card">
      <h3>clickhouse-core</h3>
      <a href="clickhouse-core/build/reports/scoverageTest/index.html">📈 View Coverage Report</a>
    </div>
    <div class="module-card">
      <h3>clickhouse-core-it</h3>
      <a href="clickhouse-core-it/build/reports/scoverageTest/index.html">📈 View Coverage Report</a>
    </div>
  </div>
  
  <h2>Spark Modules</h2>
  <div class="module-grid">
    <div class="module-card">
      <h3>Spark 3.3</h3>
      <a href="spark-3.3/clickhouse-spark/build/reports/scoverageTest/index.html">📈 clickhouse-spark Coverage</a>
      <a href="spark-3.3/clickhouse-spark-it/build/reports/scoverageTest/index.html">📈 clickhouse-spark-it Coverage</a>
    </div>
    <div class="module-card">
      <h3>Spark 3.4</h3>
      <a href="spark-3.4/clickhouse-spark/build/reports/scoverageTest/index.html">📈 clickhouse-spark Coverage</a>
      <a href="spark-3.4/clickhouse-spark-it/build/reports/scoverageTest/index.html">📈 clickhouse-spark-it Coverage</a>
    </div>
    <div class="module-card">
      <h3>Spark 3.5</h3>
      <a href="spark-3.5/clickhouse-spark/build/reports/scoverageTest/index.html">📈 clickhouse-spark Coverage</a>
      <a href="spark-3.5/clickhouse-spark-it/build/reports/scoverageTest/index.html">📈 clickhouse-spark-it Coverage</a>
    </div>
  </div>
  
  <p class="timestamp">
    Reports generated locally for testing.<br>
    View the <a href="https://github.com/ClickHouse/spark-clickhouse-connector">source repository</a>.
  </p>
</body>
</html>
EOF

echo "✅ GitHub Pages structure created in gh-pages/"
echo ""

# Step 4: Summary
echo "🎉 Done! Here's what was created:"
echo ""
echo "📁 Coverage Reports:"
echo "  - clickhouse-core/build/reports/scoverageTest/index.html"
echo "  - spark-${SPARK_VERSION}/clickhouse-spark/build/reports/scoverageTest/index.html"
echo ""
echo "📁 GitHub Pages Preview:"
echo "  - gh-pages/index.html (landing page)"
echo "  - gh-pages/clickhouse-core/"
echo "  - gh-pages/spark-*/"
echo ""
echo "🌐 To preview locally:"
echo "  cd gh-pages && python3 -m http.server 8000"
echo "  Then open: http://localhost:8000"
echo ""
echo "📤 To deploy to GitHub Pages:"
echo "  1. Commit the gh-pages directory (or let CI do it)"
echo "  2. Enable GitHub Pages in your fork's settings"
echo "  3. Set source to 'gh-pages' branch"
echo ""
