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

The project has no available binary release now, you must build from source code before using, and Java 8 is required
for building. 

Check out source code from GitHub.

```
git checkout https://github.com/housepower/spark-clickhouse-connector.git
```

Build.

```shell
./gradlew clean build -x test
```

Go to `clickhouse-spark-32-runtime/build/libs/` to find the output jar `clickhouse-spark-32-runtime_2.12-{version}.jar`.
