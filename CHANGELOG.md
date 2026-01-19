<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.10.0] - 2026-01-19

Changes that have been merged but not yet released will be documented here.

### Added
- Variant type support ([#456](https://github.com/ClickHouse/spark-clickhouse-connector/pull/456)). Added support for Spark 4.0's `VariantType` mapped to ClickHouse's `JSON`/`Variant` type. Requires Spark 4.0+ and ClickHouse 25.3+.
- TableProvider API support ([#471](https://github.com/ClickHouse/spark-clickhouse-connector/pull/471)). Added `ClickHouseTableProvider` implementation enabling format-based access pattern (`.format("clickhouse")`), making the connector compatible with Databricks Unity Catalog and other environments that require TableProvider API.
- Support for macros in cluster names ([#400](https://github.com/ClickHouse/spark-clickhouse-connector/pull/400)). The connector now resolves ClickHouse macros (e.g., `{cluster}`) in Distributed table cluster names, allowing dynamic cluster resolution based on node-specific macro definitions.

### Changed
- **Breaking Change**: Default value of `spark.clickhouse.ignoreUnsupportedTransform` changed from `false` to `true` ([#499](https://github.com/ClickHouse/spark-clickhouse-connector/pull/499)). Writes to tables with unsupported partition/sharding expressions (e.g., `PARTITION BY tuple()`, `PARTITION BY substring(...)`) now log a warning and continue instead of failing. To restore the old fail-fast behavior, set `spark.clickhouse.ignoreUnsupportedTransform=false`. **Warning**: For Distributed tables with `spark.clickhouse.write.distributed.convertLocal=true`, unsupported sharding keys may cause data corruption. The connector validates this and throws an error by default. To allow it, explicitly set `spark.clickhouse.write.distributed.convertLocal.allowUnsupportedSharding=true`.

### Fixed
- Fixed UInt64 type mapping to prevent data loss and overflow ([#477](https://github.com/ClickHouse/spark-clickhouse-connector/pull/477)). ClickHouse `UInt64` type (range: 0 to 18446744073709551615) is now mapped to Spark `DecimalType(20, 0)` instead of `LongType` to safely handle the full value range without overflow. Previously, values greater than 9223372036854775807 (Long.MAX_VALUE) would overflow or lose precision.

## [0.9.0] - 2025-12-01

### Added
- Support for Spark 4.0 ([#452](https://github.com/ClickHouse/spark-clickhouse-connector/pull/452)). This enables compatibility with the latest Spark version, allowing users to leverage new Spark 4.0 features and improvements.
- Support for reading with settings ([#367](https://github.com/ClickHouse/spark-clickhouse-connector/pull/367)). Users can now pass ClickHouse settings when reading data, providing more control over query execution.
- Binary reader supports String for Array/Map reading ([#395](https://github.com/ClickHouse/spark-clickhouse-connector/pull/395)). Improved handling of String types when reading Array and Map columns in binary format.
- Struct type support ([#453](https://github.com/ClickHouse/spark-clickhouse-connector/pull/453)). Added support for reading and writing ClickHouse Struct types, expanding the range of supported data types.

### Changed
- Updated ClickHouse Java client version to 0.9.4 ([#428](https://github.com/ClickHouse/spark-clickhouse-connector/pull/428)). This update brings bug fixes and improvements from the latest ClickHouse Java client.
- Aligned Spark 4.0 dependencies with Spark 4.0.1 release ([#458](https://github.com/ClickHouse/spark-clickhouse-connector/pull/458)). Ensures compatibility with the stable Spark 4.0.1 release.
- Updated supported ClickHouse versions ([#425](https://github.com/ClickHouse/spark-clickhouse-connector/pull/425)). Updated the list of tested and supported ClickHouse server versions.

### Fixed
- Fixed tests and SonarQube configuration ([#419](https://github.com/ClickHouse/spark-clickhouse-connector/pull/419), [#422](https://github.com/ClickHouse/spark-clickhouse-connector/pull/422), [#423](https://github.com/ClickHouse/spark-clickhouse-connector/pull/423)). Resolved issues with test execution and code quality analysis tools.
- Fixed resources configuration ([#426](https://github.com/ClickHouse/spark-clickhouse-connector/pull/426)). Corrected resource file handling in the build process.

## [0.8.1] - 2024-11-05

### Fixed
- Protect from ArrayIndexOutOfBoundsException exception when extracting values for user agent ([#360](https://github.com/ClickHouse/spark-clickhouse-connector/pull/360)). Fixed a potential crash when parsing user agent strings, improving stability.

## [0.8.0] - 2024-08-12

### Added
- Support for Spark 3.5. Added compatibility with Apache Spark 3.5, expanding the range of supported Spark versions.
- Added dedicated user agent. The connector now identifies itself with a specific user agent string for better tracking and debugging.

### Changed
- Upgraded to Java client version 0.6.3. Updated to a newer version of the ClickHouse Java client with improved stability and features.
- Tested against cloud. The connector has been validated against ClickHouse Cloud environments.

### Removed
- gRPC support is removed, now HTTP is the only option. The gRPC protocol has been deprecated and removed in favor of HTTP, which provides better compatibility and is the recommended protocol.

### Breaking Changes
- Project groupId is renamed from `com.github.housepower` to `com.clickhouse.spark`. Users need to update their dependency coordinates when upgrading.
- Class `xenon.clickhouse.ClickHouseCatalog` is renamed to `com.clickhouse.spark.ClickHouseCatalog`. Code using the old class name needs to be updated to the new package structure.

## [0.7.3] - 2024-02-06

### Changed
- Compatible with Spark 3.3, 3.4
- Uses ClickHouse JDBC version 0.4.6

## [0.6.1] - 2023-04-27

### Fixed
- Spark 3.3: Fix custom options

## [0.6.0] - 2023-03-13

### Added
- Support for reading Bool type
- Support for RowBinary format in reading
- Support for read metrics
- Support `Date` type as partition column in `dropPartition`
- Support `tcp_port` in catalog option
- Allow setting arbitrary options for ClickHouseClient

### Changed
- Changed default protocol to HTTP
- Simplified spark.clickhouse.write.format values
- Use clickhouse java client to parse schema
- Renamed and reorganized functions

### Fixed
- Fixed Decimal reading in JSON format
- Fixed timestamp value transformation
- Respect ClickHouse ORDER BY Clause default behavior

## [0.5.0] - 2022-08-09

### Added
- Switched from ClickHouse raw gRPC Client to ClickHouse Official Java Client
- HTTP protocol support added
- Support compression on reading
- Support write metrics
- Add column comment when create clickhouse table

### Changed
- Extended range of supported ClickHouse Server versions
- Use ClickHouse Java client
- Support compression on reading

### Removed
- gzip and zstd write compression support has been removed (currently supported codecs are `none`, `lz4` (default))

[Unreleased]: https://github.com/ClickHouse/spark-clickhouse-connector/compare/v0.9.0...HEAD
[0.9.0]: https://github.com/ClickHouse/spark-clickhouse-connector/releases/tag/v0.9.0
[0.8.1]: https://github.com/ClickHouse/spark-clickhouse-connector/releases/tag/v0.8.1
[0.8.0]: https://github.com/ClickHouse/spark-clickhouse-connector/releases/tag/v0.8.0
[0.7.3]: https://github.com/ClickHouse/spark-clickhouse-connector/releases/tag/v0.7.3
[0.6.1]: https://github.com/ClickHouse/spark-clickhouse-connector/releases/tag/v0.6.1
[0.6.0]: https://github.com/ClickHouse/spark-clickhouse-connector/releases/tag/v0.6.0
[0.5.0]: https://github.com/ClickHouse/spark-clickhouse-connector/releases/tag/v0.5.0

