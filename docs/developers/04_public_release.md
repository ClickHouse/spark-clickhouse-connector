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

Public Release
===

!!! danger "Notice"

    Public Release means deploying to Maven Central. Only core team members are granted to deploy into Public Repository.

!!! note

    Most of the steps for a public release are done by the GitHub workflow.

## Snapshot Release

The daily snapshot release is managed by [Publish Snapshot](https://github.com/ClickHouse/spark-clickhouse-connector/blob/main/.github/workflows/publish-snapshot.yml)
workflow, it is scheduled to be deployed at midnight every day.


## Feature Release

1. Cut new branch from main branch, e.g. `branch-0.3`;
2. Update version in `version.txt` and `docker/.env-dev`, e.g. from `0.3.0-SNAPSHOT` to `0.3.0`;
3. Create new tag, e.g. `v0.3.0`, it will trigger the [Publish Release](https://github.com/ClickHouse/spark-clickhouse-connector/blob/main/.github/workflows/publish-release.yml)
   workflow; 
4. Verify, close, and release in [Sonatype Repository](https://oss.sonatype.org/#stagingRepositories)
5. Announce in [GitHub Release](https://github.com/ClickHouse/spark-clickhouse-connector/releases)
6. Update version in `version.txt` and `docker/.env-dev`, e.g. from `0.3.0` to `0.3.1-SNAPSHOT`;
7. Update version on main branch in `version.txt` and `docker/.env-dev`, e.g. from `0.3.0-SNAPSHOT` to `0.4.0-SNAPSHOT`;
8. [Publish Docker image](https://github.com/ClickHouse/spark-clickhouse-connector/tree/main/docker) after jars
   available in Maven Central, generally it costs few minutes after step 3.

## Patch Release

Just emit step 1 and step 7 from feature release.
