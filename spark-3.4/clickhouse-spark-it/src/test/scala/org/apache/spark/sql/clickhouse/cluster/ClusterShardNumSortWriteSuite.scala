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

package org.apache.spark.sql.clickhouse.cluster

import com.clickhouse.spark.{ClickHouseTable, ClickHouseTableProvider}
import com.clickhouse.spark.spec.{ClusterSpec, NodeSpec, ReplicaSpec, ShardSpec, ShardUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.clickhouse.TestUtils.om
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Verifies that `convertLocal` writes sort rows by shard number, so each writer task issues
 * about one INSERT per shard instead of one INSERT per distinct sharding key value.
 */
class ClusterShardNumSortWriteSuite extends SparkClickHouseClusterTest {

  private val numSparkPartitions = 2

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.shuffle.partitions", numSparkPartitions.toString)
    .set("spark.clickhouse.write.batchSize", "100000")
    .set("spark.clickhouse.write.distributed.useClusterNodes", "true")
    .set("spark.clickhouse.write.distributed.convertLocal", "true")

  // mirrors the `single_replica` cluster topology: 4 shards, weight 1 each
  private val singleReplicaClusterSpec: ClusterSpec = ClusterSpec(
    "single_replica",
    Array(
      ShardSpec(1, 1, Array(ReplicaSpec(1, NodeSpec("clickhouse-s1r1")))),
      ShardSpec(2, 1, Array(ReplicaSpec(1, NodeSpec("clickhouse-s1r2")))),
      ShardSpec(3, 1, Array(ReplicaSpec(1, NodeSpec("clickhouse-s2r1")))),
      ShardSpec(4, 1, Array(ReplicaSpec(1, NodeSpec("clickhouse-s2r2"))))
    )
  )

  // Spark catalog per shard num of the `single_replica` cluster
  private val shardCatalogs: Map[Int, String] = Map(
    1 -> "clickhouse_s1r1",
    2 -> "clickhouse_s1r2",
    3 -> "clickhouse_s2r1",
    4 -> "clickhouse_s2r2"
  )

  private def shardNodeOptions: Map[Int, Map[String, String]] = Map(
    1 -> (clickhouse_s1r1_host, clickhouse_s1r1_http_port),
    2 -> (clickhouse_s1r2_host, clickhouse_s1r2_http_port),
    3 -> (clickhouse_s2r1_host, clickhouse_s2r1_http_port),
    4 -> (clickhouse_s2r2_host, clickhouse_s2r2_http_port)
  ).map { case (shardNum, (host, httpPort)) =>
    shardNum -> Map(
      "host" -> host,
      "http_port" -> httpPort.toString,
      "protocol" -> "http",
      "user" -> "default",
      "password" -> "",
      "database" -> "default"
    )
  }

  private def resetQueryLog(nodeOptions: Map[String, String]): Unit = {
    runClickHouseSQL("SYSTEM FLUSH LOGS", nodeOptions)
    runClickHouseSQL("TRUNCATE TABLE IF EXISTS system.query_log", nodeOptions)
  }

  private def countLocalInserts(nodeOptions: Map[String, String], db: String, tbl: String): Long = {
    runClickHouseSQL("SYSTEM FLUSH LOGS", nodeOptions)
    val json = runClickHouseSQL(
      s"""SELECT count() AS cnt FROM system.query_log
         | WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND has(tables, '$db.$tbl')
         |""".stripMargin,
      nodeOptions
    ).head.getString(0)
    om.readTree(json).get("cnt").asText.toLong
  }

  test("convertLocal write batches per shard instead of per distinct sharding key") {
    val numRows = 1000
    val distinctKeys = 251
    withSimpleDistTable("single_replica", "db_shard_num_sort", "t_dist") { (_, db, tbl_dist, tbl_local) =>
      // reset the query logs so only this test's INSERTs are counted
      shardNodeOptions.values.foreach(resetQueryLog)

      val tblSchema = spark.table(s"$db.$tbl_dist").schema
      val df = spark.range(1, numRows + 1).toDF("id")
        .withColumn("create_time", lit(timestamp("2024-01-01T00:00:00Z")))
        .withColumn("y", (col("id") % distinctKeys).cast("int"))
        .withColumn("m", (col("id") % 3 + 1).cast("int"))
        .withColumn("value", col("id").cast("string"))
        .select("create_time", "y", "m", "id", "value")
      spark.createDataFrame(df.rdd, tblSchema)
        .writeTo(s"$db.$tbl_dist")
        .append()
      Thread.sleep(2000)

      // every row landed on the shard `ShardUtils.calcShard` predicts for its sharding key
      val expectedIdsByShard = (1L to numRows)
        .groupBy(id => ShardUtils.calcShard(singleReplicaClusterSpec, id % distinctKeys).num)
        .map { case (shardNum, ids) => shardNum -> ids.toSet }
      val localRowCounts = shardCatalogs.map { case (shardNum, catalog) =>
        val localIds = spark.table(s"$catalog.$db.$tbl_local")
          .select("id").collect().map(_.getLong(0)).toSet
        assert(localIds === expectedIdsByShard.getOrElse(shardNum, Set.empty[Long]), s"shard $shardNum")
        localIds.size
      }
      assert(localRowCounts.sum === numRows)

      // rows arrive sorted by shard number, so each writer task issues about one INSERT per
      // shard; without the shard-num sort it would be about one INSERT per distinct key value
      val totalInserts = shardNodeOptions.values.map(countLocalInserts(_, db, tbl_local)).sum
      val numShards = shardCatalogs.size
      assert(totalInserts >= numShards, s"expected at least one INSERT per shard, got $totalInserts")
      assert(
        totalInserts <= numShards * numSparkPartitions + 2,
        s"expected about one INSERT per shard per Spark partition, got $totalInserts"
      )
      assert(
        totalInserts < distinctKeys / 4,
        s"expected far fewer INSERTs than the $distinctKeys distinct sharding key values, got $totalInserts"
      )
    }
  }

  test("format() write with convertLocal falls back to the sharding-key sort") {
    val numRows = 1000
    val distinctKeys = 251
    withSimpleDistTable("single_replica", "db_shard_num_sort_fmt", "t_dist") { (_, db, tbl_dist, tbl_local) =>
      // catalog-loaded tables can resolve catalog functions at plan time
      val catalogTable = spark.sessionState.catalogManager.catalog("clickhouse_s1r1")
        .asInstanceOf[TableCatalog]
        .loadTable(Identifier.of(Array(db), tbl_dist))
        .asInstanceOf[ClickHouseTable]
      assert(catalogTable.functionCatalogUsable)

      // TableProvider-loaded tables cannot: their relations carry no catalog, so the provider
      // stamps the table and `WriteJobDescription` falls back to the raw sharding-key sort
      // (`WriteJobDescriptionSuite` pins the flag -> sort-order mapping)
      val providerOptions = new java.util.HashMap[String, String]()
      providerOptions.put("host", clickhouse_s1r1_host)
      providerOptions.put("http_port", clickhouse_s1r1_http_port.toString)
      providerOptions.put("protocol", "http")
      providerOptions.put("user", "default")
      providerOptions.put("password", "")
      providerOptions.put("database", db)
      providerOptions.put("table", tbl_dist)
      val provider = new ClickHouseTableProvider
      val providerSchema = provider.inferSchema(new CaseInsensitiveStringMap(providerOptions))
      val providerTable = provider.getTable(providerSchema, Array.empty[Transform], providerOptions)
        .asInstanceOf[ClickHouseTable]
      assert(!providerTable.functionCatalogUsable)

      // the fallback write plans (no unresolvable `clickhouse_shard_num` in the sort) and lands
      // every row on the shard `ShardUtils.calcShard` predicts for its sharding key
      val tblSchema = spark.table(s"$db.$tbl_dist").schema
      val df = spark.range(1, numRows + 1).toDF("id")
        .withColumn("create_time", lit(timestamp("2024-01-01T00:00:00Z")))
        .withColumn("y", (col("id") % distinctKeys).cast("int"))
        .withColumn("m", (col("id") % 3 + 1).cast("int"))
        .withColumn("value", col("id").cast("string"))
        .select("create_time", "y", "m", "id", "value")
      spark.createDataFrame(df.rdd, tblSchema)
        .write.format("clickhouse")
        .options(providerOptions)
        .mode("append")
        .save()

      val expectedIdsByShard = (1L to numRows)
        .groupBy(id => ShardUtils.calcShard(singleReplicaClusterSpec, id % distinctKeys).num)
        .map { case (shardNum, ids) => shardNum -> ids.toSet }
      val localRowCounts = shardCatalogs.map { case (shardNum, catalog) =>
        val localIds = spark.table(s"$catalog.$db.$tbl_local")
          .select("id").collect().map(_.getLong(0)).toSet
        assert(localIds === expectedIdsByShard.getOrElse(shardNum, Set.empty[Long]), s"shard $shardNum")
        localIds.size
      }
      assert(localRowCounts.sum === numRows)
    }
  }
}
