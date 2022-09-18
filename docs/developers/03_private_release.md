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

Private Release
===

!!! tip

    Internal Release means deploying to private Nexus Repository. Please make sure you are granted to access your
    company private Nexus Repository.

### Repository and Authentication 

Configure Gradle in `~/.gradle/gradle.properties`.

```
mavenUser=xxx
mavenPassword=xxx
mavenReleasesRepo=xxx
mavenSnapshotsRepo=xxx
```

### Upgrade Version

Modify version in `version.txt` and `docker/.env-dev`

### Build and Deploy

Publish to Maven Repository using `./gradlew publish`
