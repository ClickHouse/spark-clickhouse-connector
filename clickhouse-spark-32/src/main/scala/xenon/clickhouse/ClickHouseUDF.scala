package xenon.clickhouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{InterpretedHashFunction, XxHash64Function}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import java.lang.{Long => Int64}

object ClickHouseUDF {

  def register(implicit spark: SparkSession): Unit = {
    spark.udf.register("ck_xx_hash64", ck_xx_hash64)
    spark.udf.register("ck_xx_hash64_shard", ck_xx_hash64_shard)
  }

  /**
   * ClickHouse equivalent SQL:
   * {{{
   *   select xxHash64( concat(project_id, toString(seq) )
   * }}}
   */
  val ck_xx_hash64: UserDefinedFunction = ckHashUDF(XxHash64Function)

  /**
   * Create ClickHouse table with DDL:
   * {{{
   * CREATE TABLE ON CLUSTER cluster (
   *   ...
   * ) ENGINE = Distributed(
   *    cluster,
   *    db,
   *    local_table,
   *    xxHash64( concat(project_id, project_version, toString(seq) )
   * );
   * }}}
   */
  val ck_xx_hash64_shard: UserDefinedFunction = ckHashShardUDF(XxHash64Function)

  def ckHashUDF(hashFun: => InterpretedHashFunction): UserDefinedFunction = udf { str: String =>
    hashFun.hash(UTF8String.fromString(str), StringType, 0L)
  }

  // assume that all shards has same weight
  def ckHashShardUDF(hashFun: => InterpretedHashFunction): UserDefinedFunction = udf { (num: Int, str: String) =>
    val hash: Long = hashFun.hash(UTF8String.fromString(str), StringType, 0L)
    Int64.remainderUnsigned(hash, num)
  }
}
