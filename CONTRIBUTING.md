# Contributing to Spark ClickHouse Connector

Thank you for considering contributing to the Spark ClickHouse Connector! We value your contributions and appreciate your efforts to improve the connector. This guide will help you get started with the contribution process.

## Introduction

The Spark ClickHouse Connector is built on Apache Spark DataSourceV2 API and enables seamless integration between Apache Spark and ClickHouse. Contributions of all kinds are welcome, including bug fixes, new features, documentation improvements, and test enhancements.

## Getting Started

### 1. Fork the Repository

Start by forking the repository on GitHub. This will create a copy of the repository under your own GitHub account.

### 2. Set Up Your Development Environment

#### Prerequisites

* **Java**: Java 8 or 17 (check with `java -version`)
* **Scala**: Scala 2.12 or 2.13
* **Gradle**: The project includes a Gradle wrapper (`./gradlew`), so you don't need to install Gradle separately
* **Docker**: Required for running integration tests. Install [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/)
* **Git**: For version control

#### Clone Your Fork

```bash
git clone https://github.com/YOUR_USERNAME/spark-clickhouse-connector.git
cd spark-clickhouse-connector
```

#### Set Up Upstream Remote

```bash
git remote add upstream https://github.com/ClickHouse/spark-clickhouse-connector.git
```

### 3. Build the Project

Build without running tests:

```bash
./gradlew clean build -x test
```

The output JAR files will be located in:
- `spark-{{ spark_binary_version }}/clickhouse-spark-runtime/build/libs/`

For example: `spark-3.5/clickhouse-spark-runtime/build/libs/clickhouse-spark-runtime-3.5_2.12-0.9.0-SNAPSHOT.jar`

### 4. Create a Branch

Create a new branch for your feature or bug fix. We recommend using descriptive branch names:

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-description
```

## Making Changes

### Code Style

* Follow the existing code style and conventions used in the project
* The project uses [Spotless](https://github.com/diffplug/spotless) for code formatting
* Run `./gradlew spotlessApply` to format your code before committing

### Commit Messages

We strongly recommend following the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification:

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types include:
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

Example:
```
feat(write): add support for batch insert optimization

This change improves write performance by batching inserts
when writing large datasets to ClickHouse.

Closes #123
```

## Testing

### Running Tests

The project uses [Testcontainers](https://www.testcontainers.org/) and [Docker Compose](https://docs.docker.com/compose/) for integration tests.

#### Run All Tests

```bash
./gradlew clean test
```

#### Run Tests for Specific Spark Version

```bash
./gradlew clean test -Dspark_binary_version=3.5 -Dscala_binary_version=2.13
```

#### Run a Single Test

```bash
./gradlew test --tests=ConvertDistToLocalWriteSuite
```

#### Test Against Custom ClickHouse Image

```bash
CLICKHOUSE_IMAGE=custom-org/clickhouse-server:custom-tag ./gradlew test
```

### Writing Tests

* Add tests for new features and bug fixes
* Ensure all existing tests pass before submitting your PR
* Tests are located in the `*-it` modules (integration tests) and test directories
* Follow the existing test patterns and structure

### Test Requirements

> **Important:** Please make sure all tests pass before pushing your code.

## Submitting Changes

### 1. Update Documentation

If your changes affect user-facing functionality:
* Update user-facing documentation in the [ClickHouse documentation repository](https://github.com/ClickHouse/clickhouse-docs)
* Update developer documentation in the local `docs/` directory if your changes affect build, testing, or release processes
* Update the README if necessary
* Add examples if introducing new features

### 2. Update CHANGELOG.md

Add an entry to `CHANGELOG.md` describing your changes. Place it under the `[Unreleased]` section:

```markdown
## [Unreleased]

### Added
- New feature: support for batch insert optimization

### Fixed
- Fixed issue with connection timeout handling
```

### 3. Create a Pull Request

1. Push your branch to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

2. Create a pull request from your forked repository to the main repository

3. In your PR description, include:
   * A clear description of the changes
   * Link to related issues (e.g., "Closes #123")
   * Any breaking changes or migration notes
   * Screenshots or examples if applicable

4. Ensure your PR:
   * Passes all CI checks
   * Has been reviewed and approved
   * Is up to date with the main branch

### 4. Sign the CLA

If this is your first contribution, make sure to sign ClickHouse's Contributor License Agreement (CLA) when prompted.

## Development Guidelines

### Project Structure

* `clickhouse-core/`: Core functionality shared across Spark versions
* `spark-3.3/`, `spark-3.4/`, `spark-3.5/`, `spark-4.0/`: Spark version-specific implementations
* `docs/`: Documentation
* `docker/`: Docker configuration for development and testing

### Supported Spark Versions

The connector supports:
* Apache Spark 3.3, 3.4, 3.5, and 4.0
* Scala 2.12 and 2.13

### Code Review Process

* All PRs require at least one approval from a maintainer
* Address review comments promptly
* Be open to feedback and suggestions
* Keep PRs focused and reasonably sized

## Getting Help

* **Issues**: Use GitHub Issues to report bugs or request features
* **Discussions**: Use GitHub Discussions for questions and general discussion
* **Documentation**: Check the [documentation](https://clickhouse.com/docs/en/integrations/apache-spark) for usage examples

## Release Process

Releases are managed by the core team.

## Code of Conduct

Please be respectful and considerate of others when contributing. We aim to maintain a welcoming and inclusive community.

## Thank You!

Your contributions help make the Spark ClickHouse Connector better for everyone. Thank you for taking the time to contribute!

