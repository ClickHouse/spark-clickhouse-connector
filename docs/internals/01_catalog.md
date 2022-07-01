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

Catalog Management
===

One important end user facing feature of DataSource V2 is supporting of multi-catalogs.

In the early stage of Spark, it does have catalog concept, usually, user use Hive Metastore or Glue to manage table
metadata, hence user must register external DataSource tables in centralized metastore before using it.

<figure markdown>
  ![Overview](../imgs/spark_centralized_metastore.drawio.png)
</figure>

In the centralized metastore model, a table is identified by `<database>.<table>`.

Things changed in DataSource V2, starting from v3.0.0, Spark introduced catalog concept, then a table is identified by
`<catalog>.<database>.<table>`.

<figure markdown>
  ![Overview](../imgs/spark_multi_catalog.drawio.png)
</figure>

The default catalog has a fixed name `spark_catalog`.
