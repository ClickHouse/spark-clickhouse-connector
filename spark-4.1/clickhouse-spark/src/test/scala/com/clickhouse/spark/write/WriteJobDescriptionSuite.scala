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

package com.clickhouse.spark.write

import com.clickhouse.spark.expr.FieldRef
import com.clickhouse.spark.func.{ClickHouseShardNum, StaticFunctionRegistry}
import com.clickhouse.spark.spec.{ClusterSpec, DistributedEngineSpec, NodeSpec, ReplicaSpec, ShardSpec, TableSpec}
import org.apache.spark.sql.clickhouse.WriteOptions
import org.apache.spark.sql.connector.expressions.{Literal, SortOrder, Transform}
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDateTime, ZoneId}

class WriteJobDescriptionSuite extends AnyFunSuite {

  private val clusterSpec = ClusterSpec(
    "test_cluster",
    Array(
      ShardSpec(1, 1, Array(ReplicaSpec(1, NodeSpec("s1r1")))),
      ShardSpec(2, 1, Array(ReplicaSpec(1, NodeSpec("s2r1"))))
    )
  )

  test("sparkSortOrders sorts by shard num when convertLocal is set and the function catalog is usable") {
    val orders = writeJob(convertLocal = true, functionCatalogUsable = true).sparkSortOrders
    assert(orders.length === 1)
    assertSortsByShardNum(orders.head)
  }

  test("sparkSortOrders sorts by raw sharding key when the function catalog is not usable") {
    val orders = writeJob(convertLocal = true, functionCatalogUsable = false).sparkSortOrders
    assert(orders.length === 1)
    assertSortsByRawShardingKey(orders.head)
  }

  test("sparkSortOrders sorts by raw sharding key when convertLocal is not set") {
    val orders = writeJob(convertLocal = false, functionCatalogUsable = true).sparkSortOrders
    assert(orders.length === 1)
    assertSortsByRawShardingKey(orders.head)
  }

  private def assertSortsByShardNum(order: SortOrder): Unit = order.expression() match {
    case transform: Transform =>
      assert(transform.name() === ClickHouseShardNum.funcName)
      assert(transform.arguments().length === 2)
      transform.arguments()(0) match {
        case lit: Literal[_] => assert(lit.value() === "test_cluster")
        case other => fail(s"Unexpected cluster name argument: $other")
      }
      transform.arguments()(1) match {
        case shardingKey: Transform => assertIdentityTransformOnY(shardingKey)
        case other => fail(s"Unexpected sharding key argument: $other")
      }
    case other => fail(s"Unexpected sharding sort expression: $other")
  }

  private def assertSortsByRawShardingKey(order: SortOrder): Unit = order.expression() match {
    case transform: Transform => assertIdentityTransformOnY(transform)
    case other => fail(s"Unexpected sharding sort expression: $other")
  }

  private def assertIdentityTransformOnY(transform: Transform): Unit = {
    assert(transform.name() === "identity")
    assert(transform.references().map(_.fieldNames().mkString(".")).toSeq === Seq("y"))
  }

  private def writeJob(convertLocal: Boolean, functionCatalogUsable: Boolean): WriteJobDescription = {
    val optionsMap = new java.util.HashMap[String, String]()
    optionsMap.put("spark.clickhouse.write.distributed.convertLocal", convertLocal.toString)
    val engineClause = "Distributed('test_cluster', 'db', 'tbl_local', y)"
    WriteJobDescription(
      queryId = "write-job-description-suite-query",
      tableSchema = new StructType(),
      metadataSchema = new StructType(),
      dataSetSchema = new StructType(),
      node = NodeSpec("127.0.0.1"),
      tz = ZoneId.of("UTC"),
      tableSpec = TableSpec(
        database = "db",
        name = "tbl",
        uuid = "",
        engine = "Distributed",
        is_temporary = false,
        data_paths = Nil,
        metadata_path = "",
        metadata_modification_time = LocalDateTime.of(2026, 1, 1, 0, 0),
        dependencies_database = Nil,
        dependencies_table = Nil,
        create_table_query = "",
        engine_full = engineClause,
        partition_key = "",
        sorting_key = "",
        primary_key = "",
        sampling_key = "",
        storage_policy = "",
        total_rows = None,
        total_bytes = None,
        lifetime_rows = None,
        lifetime_bytes = None
      ),
      tableEngineSpec = DistributedEngineSpec(
        engine_clause = engineClause,
        cluster = "test_cluster",
        local_db = "db",
        local_table = "tbl_local",
        sharding_key = Some(FieldRef("y"))
      ),
      cluster = Some(clusterSpec),
      localTableSpec = None,
      localTableEngineSpec = None,
      shardingKey = Some(FieldRef("y")),
      partitionKey = None,
      sortingKey = None,
      writeOptions = new WriteOptions(optionsMap),
      writeSettings = Map.empty,
      functionRegistry = StaticFunctionRegistry,
      functionCatalogUsable = functionCatalogUsable
    )
  }
}
