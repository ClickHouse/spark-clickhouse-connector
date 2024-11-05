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

The name pattern of binary jar is 
```
clickhouse-spark-runtime-${spark_binary_version}_${scala_binary_version}-${version}.jar
```
you can find all available released jars under [Maven Central Repository](https://repo1.maven.org/maven2/com/clickhouse/spark)
and all daily build SNAPSHOT jars under [Sonatype OSS Snapshots Repository](https://oss.sonatype.org/content/repositories/snapshots/com/clickhouse/spark/).

## Import as Dependency

### Gradle

```
dependencies {
  implementation("com.clickhouse.spark:clickhouse-spark-runtime-{{ spark_binary_version }}_{{ scala_binary_version }}:{{ stable_version }}")
  implementation("com.clickhouse:clickhouse-jdbc:{{ clickhouse_jdbc_version }}:all") { transitive = false }
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
  <groupId>com.clickhouse.spark</groupId>
  <artifactId>clickhouse-spark-runtime-{{ spark_binary_version }}_{{ scala_binary_version }}</artifactId>
  <version>{{ stable_version }}</version>
</dependency>
<dependency>
  <groupId>com.clickhouse</groupId>
  <artifactId>clickhouse-jdbc</artifactId>
  <classifier>all</classifier>
  <version>{{ clickhouse_jdbc_version }}</version>
  <exclusions>
    <exclusion>
      <groupId>*</groupId>
      <artifactId>*</artifactId>
    </exclusion>
  </exclusions>
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
