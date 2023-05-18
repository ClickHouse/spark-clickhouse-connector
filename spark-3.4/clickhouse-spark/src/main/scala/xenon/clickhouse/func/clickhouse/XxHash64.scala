/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xenon.clickhouse.func.clickhouse

import org.apache.spark.sql.catalyst.expressions.XxHash64Function
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import xenon.clickhouse.func.ClickhouseEquivFunction
import xenon.clickhouse.spec.{ClusterSpec, ShardUtils}

/**
 * ClickHouse equivalent function:
 * {{{
 *   select xxHash64(concat(project_id, toString(seq))
 * }}}
 */
object ClickHouseXxHash64 extends UnboundFunction with ScalarFunction[Long] with ClickhouseEquivFunction {

  override def name: String = "clickhouse_xxHash64"

  override def canonicalName: String = s"clickhouse.$name"

  override val ckFuncNames: Array[String] = Array("xxHash64")

  override def description: String = s"$name: (value: string) => hash_value: long"

  override def bind(inputType: StructType): BoundFunction = inputType.fields match {
    case Array(StructField(_, StringType, _, _)) => this
    case _ => throw new UnsupportedOperationException(s"Expect 1 STRING argument. $description")
  }

  override def inputTypes: Array[DataType] = Array(StringType)

  override def resultType: DataType = LongType

  override def isResultNullable: Boolean = false

  // ignore UInt64 vs Int64
  def invoke(value: UTF8String): Long = XxHash64Function.hash(value, StringType, 0L)
}

/**
 * Create ClickHouse table with DDL:
 * {{{
 * CREATE TABLE ON CLUSTER cluster (
 *   ...
 * ) ENGINE = Distributed(
 *     cluster,
 *     db,
 *     local_table,
 *     xxHash64(concat(project_id, project_version, toString(seq))
 * );
 * }}}
 */
class ClickHouseXxHash64Shard(clusters: Seq[ClusterSpec]) extends UnboundFunction with ScalarFunction[Int] {

  @transient private lazy val indexedClusters =
    clusters.map(cluster => UTF8String.fromString(cluster.name) -> cluster).toMap

  override def name: String = "clickhouse_shard_xxHash64"

  override def canonicalName: String = s"clickhouse.$name"

  override def description: String = s"$name: (cluster_name: string, value: string) => shard_num: int"

  override def bind(inputType: StructType): BoundFunction = inputType.fields match {
    case Array(StructField(_, StringType, _, _), StructField(_, StringType, _, _)) => this
    case _ => throw new UnsupportedOperationException(s"Expect 2 STRING argument. $description")
  }

  override def inputTypes: Array[DataType] = Array(StringType, StringType)

  override def resultType: DataType = IntegerType

  override def isResultNullable: Boolean = false

  def invoke(clusterName: UTF8String, value: UTF8String): Int = {
    val clusterSpec =
      indexedClusters.getOrElse(clusterName, throw new RuntimeException(s"Unknown cluster: $clusterName"))
    val hashVal = XxHash64Function.hash(value, StringType, 0L)
    ShardUtils.calcShard(clusterSpec, hashVal).num
  }
}
