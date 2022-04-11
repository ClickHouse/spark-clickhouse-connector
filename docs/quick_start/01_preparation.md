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

Preparation
===

The project has no available binary release now, you must build from source code before using.

Also, it has not been published to Maven Central, you need to build and publish to private repository if you want
import it by using Gradle or Maven to your project.

## Build Jar from Source Code

Check out source code from GitHub.

```
git checkout https://github.com/housepower/spark-clickhouse-connector.git
```

Build.

```shell
./gradlew clean build -x test
```

Go to `spark-3.2/clickhouse-spark-runtime/build/libs/` to find the output jar `clickhouse-spark-runtime-3.2_2.12-${version}.jar`.

## Publish to Private Repository

Configure Gradle in `~/.gradle/gradle.properties`.

```
mavenUser=xxx
mavenPassword=xxx
mavenReleasesRepo=xxx
mavenSnapshotsRepo=xxx
```

Versions.

Use content of `./version.txt` if the file exists, otherwise fallback to `{year}.{month}.{date}[-SNAPSHOT]`.

Publish Snapshots.

`./gradlew publish`

Publish Release.

`./gradlew -Prelease publish`

## Import as Dependency

For Gradle.

```
dependencies {
  implementation("com.github.housepower:clickhouse-spark-runtime-3.2_2.12:${version}")
}
```

For Maven.

```
<dependency>
  <groupId>com.github.housepower</groupId>
  <artifactId>clickhouse-spark-runtime-3.2_2.12</artifactId>
  <version>${version}</version>
</dependency>
```
