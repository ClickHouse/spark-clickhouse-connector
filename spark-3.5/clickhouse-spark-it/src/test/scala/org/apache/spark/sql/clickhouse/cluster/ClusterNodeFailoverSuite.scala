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

import com.clickhouse.spark.client.NodeHealthTracker
import com.clickhouse.spark.spec.NodeSpec
import org.apache.spark.sql.Row
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.{
  READ_DISTRIBUTED_CONVERT_LOCAL,
  READ_DISTRIBUTED_USE_CLUSTER_NODES
}
import org.scalatest.BeforeAndAfterEach

/**
 * Coverage for FR #617: `read.distributed.useClusterNodes` and replica failover via
 * [[NodeHealthTracker]]. Failover against a live multi-replica topology uses the `default`
 * cluster (2 shards x 2 replicas) with ReplicatedMergeTree.
 */
class ClusterNodeFailoverSuite extends SparkClickHouseClusterTest with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    NodeHealthTracker.clear()
    super.afterEach()
  }

  test("useClusterNodes read matches convertLocal row count (no duplication)") {
    withSimpleDistTable("single_replica", "db_ucn", "t_dist", writeData = true) { (_, db, tbl_dist, _) =>
      val convertLocalCount = withSQLConf(READ_DISTRIBUTED_CONVERT_LOCAL.key -> "true") {
        spark.table(s"$db.$tbl_dist").count()
      }
      val useClusterNodesCount = withSQLConf(
        READ_DISTRIBUTED_CONVERT_LOCAL.key -> "false",
        READ_DISTRIBUTED_USE_CLUSTER_NODES.key -> "true"
      ) {
        spark.table(s"$db.$tbl_dist").count()
      }
      assert(convertLocalCount === 4)
      assert(useClusterNodesCount === convertLocalCount)
    }
  }

  test("convertLocal read survives a marked-dead replica on default cluster") {
    autoCleanupDistTable("default", "db_fo", "t_dist") { (cluster, db, tbl_dist, tbl_local) =>
      runClickHouseSQL(
        s"""CREATE TABLE IF NOT EXISTS $db.$tbl_local ON CLUSTER $cluster (
           |  id    Int64,
           |  value String
           |) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/$db/$tbl_local', '{replica}')
           | ORDER BY id
           |""".stripMargin
      )
      runClickHouseSQL(
        s"""CREATE TABLE IF NOT EXISTS $db.$tbl_dist ON CLUSTER $cluster (
           |  id    Int64,
           |  value String
           |) ENGINE = Distributed('$cluster', '$db', '$tbl_local', id)
           |""".stripMargin
      )
      runClickHouseSQL(
        s"""INSERT INTO $db.$tbl_dist VALUES
           | (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')
           |""".stripMargin
      )
      // Wait for replication to catch up on both replicas of each shard
      Thread.sleep(3000)

      // Mark s1r1 dead — convertLocal must read shard 1 via s1r2
      NodeHealthTracker.markDead(NodeSpec("clickhouse-s1r1", _http_port = Some(8123), _tcp_port = Some(9000)))

      withSQLConf(READ_DISTRIBUTED_CONVERT_LOCAL.key -> "true") {
        checkAnswer(
          spark.sql(s"SELECT id, value FROM $db.$tbl_dist ORDER BY id"),
          Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d"))
        )
      }
    }
  }
}
