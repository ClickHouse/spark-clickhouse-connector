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

Overview Design
===

In high level, Spark ClickHouse Connector is a connector build on top of Spark DataSource V2 and
ClickHouse HTTP protocol.

<figure markdown>
  ![Overview](../imgs/scc_overview.drawio.png)
</figure>
