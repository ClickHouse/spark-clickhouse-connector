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

import com.clickhouse.spark.spec.{ClusterSpec, NodeSpec, ReplicaSpec, ShardSpec, ShardUtils}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.unsafe.types.UTF8String

class ClickHouseShardNumSuite extends AnyFunSuite {

  private def shard(num: Int, weight: Int): ShardSpec =
    ShardSpec(num, weight, Array(ReplicaSpec(1, NodeSpec(s"s${num}r1"))))

  // 4 shards, weight 1 each: rem ranges [0,1) [1,2) [2,3) [3,4)
  private val uniformCluster = ClusterSpec("uniform", Array(shard(1, 1), shard(2, 1), shard(3, 1), shard(4, 1)))
  // 2 shards, weights 1 and 2: rem ranges [0,1) and [1,3)
  private val weightedCluster = ClusterSpec("weighted", Array(shard(1, 1), shard(2, 2)))

  private val func = new ClickHouseShardNum(Seq(uniformCluster, weightedCluster))

  private def invoke(cluster: String, value: Long): Int = func.invoke(UTF8String.fromString(cluster), value)

  test("invoke matches ShardUtils.calcShard on uniform cluster") {
    Seq(0L, 1L, 2L, 3L, 4L, 5L, 42L, Long.MaxValue, -1L, -2L, -5L, Long.MinValue).foreach { v =>
      assert(invoke("uniform", v) === ShardUtils.calcShard(uniformCluster, v).num, s"value: $v")
    }
    // spot-check absolute values: consecutive keys alternate shards
    assert(invoke("uniform", 3L) === 4)
    assert(invoke("uniform", 4L) === 1)
    assert(invoke("uniform", 5L) === 2)
    // remainderUnsigned: -1 as unsigned is 2^64-1, rem 3 => shard 4
    assert(invoke("uniform", -1L) === 4)
  }

  test("invoke matches ShardUtils.calcShard on non-uniform weighted cluster") {
    Seq(0L, 1L, 2L, 3L, 4L, 5L, 100L, Long.MaxValue, -1L, -7L, Long.MinValue).foreach { v =>
      assert(invoke("weighted", v) === ShardUtils.calcShard(weightedCluster, v).num, s"value: $v")
    }
    // rem 0 => shard 1, rem 1 and 2 => shard 2
    assert(invoke("weighted", 0L) === 1)
    assert(invoke("weighted", 1L) === 2)
    assert(invoke("weighted", 2L) === 2)
    assert(invoke("weighted", 3L) === 1)
  }

  test("produceResult agrees with invoke") {
    Seq(("uniform", 7L), ("weighted", 5L), ("uniform", -3L)).foreach { case (cluster, v) =>
      val row = new GenericInternalRow(Array[Any](UTF8String.fromString(cluster), v))
      assert(func.produceResult(row) === invoke(cluster, v))
    }
  }

  test("bind accepts (string, integral) arguments") {
    Seq(ByteType, ShortType, IntegerType, LongType).foreach { integralType =>
      val inputType = StructType(Seq(
        StructField("_0", StringType, nullable = false),
        StructField("_1", integralType, nullable = true)
      ))
      assert(func.bind(inputType) === func, s"type: $integralType")
    }
  }

  test("bind rejects non-integral and wrong-arity arguments") {
    Seq(
      StructType(Seq(StructField("_0", StringType), StructField("_1", StringType))),
      StructType(Seq(StructField("_0", LongType), StructField("_1", LongType))),
      StructType(Seq(StructField("_0", StringType))),
      StructType(Seq(StructField("_0", StringType), StructField("_1", LongType), StructField("_2", LongType)))
    ).foreach { inputType =>
      intercept[UnsupportedOperationException](func.bind(inputType))
    }
  }

  test("unknown cluster name throws") {
    val cause = intercept[RuntimeException](invoke("nonexistent", 1L))
    assert(cause.getMessage.contains("Unknown cluster: nonexistent"))
  }

  test("survives java serialization roundtrip") {
    val bytes = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bytes)
    oos.writeObject(func)
    oos.close()
    val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray))
    val deserialized = ois.readObject().asInstanceOf[ClickHouseShardNum]
    assert(deserialized.invoke(UTF8String.fromString("uniform"), 5L) === invoke("uniform", 5L))
  }
}
