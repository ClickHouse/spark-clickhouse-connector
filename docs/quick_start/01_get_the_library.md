---
license: |
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
      https://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  
  See the License for the specific language governing permissions and
  limitations under the License.
---

Get the Library
===

## Download the Library

The name pattern of binary jar is `clickhouse-spark-runtime-${spark_binary_version}_${scala_binary_version}-${version}.jar`,
you can find all available released jars under [Maven Central Repository](https://repo1.maven.org/maven2/com/github/housepower)
and all daily build snapshot jars under [Sonatype OSS Snapshots Repository](https://oss.sonatype.org/content/repositories/snapshots/com/github/housepower/).

!!! tip

    The runtime jar contains and relocates all required classes, you do not need to worry about transitive dependencies
    management and potential class conflictions.

## Import as Dependency

### Gradle

```
dependencies {
  implementation("com.github.housepower:clickhouse-spark-runtime-3.3_2.12:${version}")
}
```

Add the following repository if you want to use SNAPSHOT version. 

```
repositries {
  maven { url = "https://oss.sonatype.org/content/repositories/snapshots" }
}
```

### Maven

```
<dependency>
  <groupId>com.github.housepower</groupId>
  <artifactId>clickhouse-spark-runtime-3.3_2.12</artifactId>
  <version>${version}</version>
</dependency>
```

Add the following repository if you want to use SNAPSHOT version.

```
<repositories>
  <repository>
    <id>sonatype-oss-snapshots</id>
    <name>Sonatype OSS Snapshots Repository</name>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
  </repository>
</repositories>
```
