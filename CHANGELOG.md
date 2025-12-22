# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.0] - 2025-12-01

### Added
- Support for Spark 4.0
- Support for reading with settings
- Binary reader supports String for Array/Map reading
- Struct type support

### Changed
- Updated ClickHouse Java client version 0.9.4
- Aligned Spark 4.0 dependencies with Spark 4.0.1 release
- Updated supported ClickHouse versions

### Fixed
- Fixed tests and SonarQube configuration
- Fixed resources configuration

## [0.8.1] - 2024-11-05

### Fixed
- Protect from ArrayIndexOutOfBoundsException exception when extracting values for user agent

## [0.8.0] - 2024-08-12

### Added
- Support for Spark 3.5
- Added dedicated user agent

### Changed
- Upgraded to Java client version 0.6.3
- Tested against cloud

### Removed
- gRPC support is removed, now HTTP is the only option

### Breaking Changes
- Project groupId is renamed from `com.github.housepower` to `com.clickhouse.spark`
- Class `xenon.clickhouse.ClickHouseCatalog` is renamed to `com.clickhouse.spark.ClickHouseCatalog`

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

[0.9.0]: https://github.com/ClickHouse/spark-clickhouse-connector/releases/tag/v0.9.0
[0.8.1]: https://github.com/ClickHouse/spark-clickhouse-connector/releases/tag/v0.8.1
[0.8.0]: https://github.com/ClickHouse/spark-clickhouse-connector/releases/tag/v0.8.0
[0.7.3]: https://github.com/ClickHouse/spark-clickhouse-connector/releases/tag/v0.7.3
[0.6.1]: https://github.com/ClickHouse/spark-clickhouse-connector/releases/tag/v0.6.1
[0.6.0]: https://github.com/ClickHouse/spark-clickhouse-connector/releases/tag/v0.6.0
[0.5.0]: https://github.com/ClickHouse/spark-clickhouse-connector/releases/tag/v0.5.0

