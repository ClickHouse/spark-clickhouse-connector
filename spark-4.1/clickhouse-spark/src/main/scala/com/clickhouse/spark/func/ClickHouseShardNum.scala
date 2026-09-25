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

package com.clickhouse.spark.func

import com.clickhouse.spark.spec.{ClusterSpec, ShardUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object ClickHouseShardNum {
  val funcName: String = "clickhouse_shard_num"
}

/**
 * Maps an integral sharding key value to the shard number that `ShardUtils.calcShard` routes it to.
 * Used to sort rows by target shard when writing a Distributed table with
 * `spark.clickhouse.write.distributed.convertLocal=true`, so that rows of the same shard are
 * adjacent and the writer flushes full batches instead of one INSERT per distinct key value.
 */
class ClickHouseShardNum(clusters: Seq[ClusterSpec]) extends UnboundFunction with ScalarFunction[Int] {

  @transient private lazy val indexedClusters =
    clusters.map(cluster => UTF8String.fromString(cluster.name) -> cluster).toMap

  override def name: String = ClickHouseShardNum.funcName

  override def canonicalName: String = s"clickhouse.$name"

  override def description: String = s"$name: (cluster_name: string, value: byte|short|int|long) => shard_num: int"

  override def bind(inputType: StructType): BoundFunction = inputType.fields match {
    case Array(
          StructField(_, StringType, _, _),
          StructField(_, ByteType | ShortType | IntegerType | LongType, _, _)
        ) => this
    case _ => throw new UnsupportedOperationException(s"Expect (STRING, INTEGRAL) arguments. $description")
  }

  override def inputTypes: Array[DataType] = Array(StringType, LongType)

  override def resultType: DataType = IntegerType

  override def isResultNullable: Boolean = true

  def invoke(clusterName: UTF8String, value: Long): Int = {
    val clusterSpec =
      indexedClusters.getOrElse(clusterName, throw new RuntimeException(s"Unknown cluster: $clusterName"))
    ShardUtils.calcShard(clusterSpec, value).num
  }

  override def produceResult(input: InternalRow): Int = invoke(input.getUTF8String(0), input.getLong(1))
}
