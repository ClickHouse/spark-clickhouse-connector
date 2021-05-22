package xenon.clickhouse

import com.google.common.annotations.VisibleForTesting
import org.apache.spark.sql.ClickHouseAnalysisException
import xenon.clickhouse.spec._

import scala.util.matching.Regex

object TableEngineUtil {

  // part of information from `system`.`tables`

  //////////////////////////////////////
  ////////// Distribute Table //////////
  //////////////////////////////////////
  // engine:        Distributed
  // engine_full:   Distributed('default', 'wj_report', 'wj_respondent_local', xxHash64(id))
  // partition_key: <empty>
  // sorting_key:   <empty>
  // ...

  //////////////////////////////////////
  //////////// Local Table /////////////
  //////////////////////////////////////
  // engine:        ReplicatedReplacingMergeTree
  // engine_full:   ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}', ts) \
  //                PARTITION BY toYYYYMM(created) ORDER BY id SETTINGS index_granularity = 8192
  // partition_key: toYYYYMM(created)
  // sorting_key:   id
  // ...

  // TODO use antlr4 to parse expressions
  // regex base parser limitation: for some engine like ReplacingMergeTree, assume those PARTITION BY, ORDER BY, etc.
  // clause present in ordered.

  val merge_tree_engine_full_regex: Regex =
    """MergeTree\(\)""".r

  val replicated_merge_tree_engine_full_regex: Regex =
    """ReplicatedMergeTree\('([/\w{}]+)',\s*'([\w{}]+)'\)""".r

  val replacing_merge_tree_engine_full_regex: Regex =
    """ReplacingMergeTree\(([^,]+)?\)""".r

  val replicated_replacing_merge_tree_engine_full_regex: Regex =
    """ReplicatedReplacingMergeTree\('([/\w{}]+)',\s*'([\w{}]+)'(,\s*([^,]+))?\)""".r

  val distributed_engine_full_regex: Regex =
    """Distributed\('(\w+)',\s*'(\w+)',\s*'(\w+)'(,\s*([^,]+)(,\s*([^,]+))?)?\)""".r

  // noinspection DuplicatedCode
  def resolveTableEngine(tableSpec: TableSpec): TableEngineSpec =
    tableSpec.engine match {
      case "MergeTree" =>
        val engineSpec = parseMergeTreeEngine(tableSpec.engine_full)
        engineSpec.copy(
          sorting_key = tableSpec.sorting_key,
          partition_key = Some(tableSpec.partition_key).filter(_.nonEmpty),
          primary_key = Some(tableSpec.primary_key).filter(_.nonEmpty),
          sampling_key = Some(tableSpec.sampling_key).filter(_.nonEmpty)
        )
      case "ReplicatedMergeTree" =>
        val engineSpec = parseReplicatedMergeTreeEngine(tableSpec.engine_full)
        engineSpec.copy(
          sorting_key = tableSpec.sorting_key,
          partition_key = Some(tableSpec.partition_key).filter(_.nonEmpty),
          primary_key = Some(tableSpec.primary_key).filter(_.nonEmpty),
          sampling_key = Some(tableSpec.sampling_key).filter(_.nonEmpty)
        )
      case "ReplacingMergeTree" =>
        val engineSpec = parseReplacingMergeTreeEngine(tableSpec.engine_full)
        engineSpec.copy(
          sorting_key = tableSpec.sorting_key,
          partition_key = Some(tableSpec.partition_key).filter(_.nonEmpty),
          primary_key = Some(tableSpec.primary_key).filter(_.nonEmpty),
          sampling_key = Some(tableSpec.sampling_key).filter(_.nonEmpty)
        )
      case "ReplicatedReplacingMergeTree" =>
        val engineSpec = parseReplicatedReplacingMergeTreeEngine(tableSpec.engine_full)
        engineSpec.copy(
          sorting_key = tableSpec.sorting_key,
          partition_key = Some(tableSpec.partition_key).filter(_.nonEmpty),
          primary_key = Some(tableSpec.primary_key).filter(_.nonEmpty),
          sampling_key = Some(tableSpec.sampling_key).filter(_.nonEmpty)
        )
      case "Distributed" =>
        parseDistributedEngine(tableSpec.engine_full)
      case engine =>
        UnknownTableEngineSpec(engine)
    }

  def resolveTableCluster(distributedEngineSpec: DistributedEngineSpec, clusterSpecs: Seq[ClusterSpec]): ClusterSpec =
    clusterSpecs.find(_.name == distributedEngineSpec.cluster)
      .getOrElse(throw ClickHouseAnalysisException(s"unknown cluster: ${distributedEngineSpec.cluster}"))

  /////////////////////////////////////////////
  ////////// Parse Engine Expression //////////
  /////////////////////////////////////////////
  /**
   * @param engine_full
   *   e.g. `MergeTree()`
   * @return
   *   [[MergeTreeEngineSpec]]
   */
  @VisibleForTesting
  private[clickhouse] def parseMergeTreeEngine(engine_full: String): MergeTreeEngineSpec =
    engine_full match {
      case merge_tree_engine_full_regex() =>
        MergeTreeEngineSpec(engine_full)
      case _ => throw ClickHouseAnalysisException(s"parse full engine failed. $engine_full")
    }

  /**
   * This method does NOT resolve marcos
   * @param engine_full
   *   e.g. `ReplicatedMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}')`
   * @return
   *   [[ReplicatedMergeTreeEngineSpec]]
   */
  @VisibleForTesting
  private[clickhouse] def parseReplicatedMergeTreeEngine(engine_full: String): ReplicatedMergeTreeEngineSpec =
    engine_full match {
      case replicated_merge_tree_engine_full_regex(zk_path, replica_name) =>
        ReplicatedMergeTreeEngineSpec(engine_full, zk_path, replica_name)
      case _ => throw ClickHouseAnalysisException(s"parse full engine failed. $engine_full")
    }

  /**
   * @param engine_full
   *   e.g. `ReplacingMergeTree(ts)`
   * @return
   *   [[ReplacingMergeTreeEngineSpec]]
   */
  @VisibleForTesting
  private[clickhouse] def parseReplacingMergeTreeEngine(engine_full: String): ReplacingMergeTreeEngineSpec =
    engine_full match {
      case replacing_merge_tree_engine_full_regex() =>
        ReplacingMergeTreeEngineSpec(engine_full)
      case replacing_merge_tree_engine_full_regex(version_col) =>
        ReplacingMergeTreeEngineSpec(engine_full, Option(version_col))
      case _ => throw ClickHouseAnalysisException(s"parse full engine failed. $engine_full")
    }

  /**
   * This method does NOT resolve marcos
   * @param engine_full
   *   e.g. ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/wj_report/wj_respondent', '{replica}', ts)
   * @return
   *   [[ReplicatedReplacingMergeTreeEngineSpec]]
   */
  @VisibleForTesting
  private[clickhouse] def parseReplicatedReplacingMergeTreeEngine(engine_full: String)
    : ReplicatedReplacingMergeTreeEngineSpec =
    engine_full match {
      case replicated_replacing_merge_tree_engine_full_regex(zk_path, replica_name) =>
        ReplicatedReplacingMergeTreeEngineSpec(engine_full, zk_path, replica_name)
      case replicated_replacing_merge_tree_engine_full_regex(zk_path, replica_name, _, version_col) =>
        ReplicatedReplacingMergeTreeEngineSpec(engine_full, zk_path, replica_name, Option(version_col))
      case _ => throw ClickHouseAnalysisException(s"parse full engine failed. $engine_full")
    }

  /**
   * This method does NOT validate cluster
   *
   * @param engine_full
   *   e.g. `Distributed('default', 'wj_report', 'wj_respondent_local', xxHash64(id))`
   * @return
   *   [[DistributedEngineSpec]]
   */
  @VisibleForTesting
  private[clickhouse] def parseDistributedEngine(engine_full: String): DistributedEngineSpec =
    engine_full match {
      case distributed_engine_full_regex(cluster, local_db, local_tbl) =>
        DistributedEngineSpec(engine_full, cluster, local_db, local_tbl)
      case distributed_engine_full_regex(cluster, local_db, local_tbl, _, sharding_key) =>
        DistributedEngineSpec(engine_full, cluster, local_db, local_tbl, Option(sharding_key))
      case distributed_engine_full_regex(cluster, local_db, local_tbl, _, sharding_key, _, policy) =>
        DistributedEngineSpec(engine_full, cluster, local_db, local_tbl, Option(sharding_key), Option(policy))
      case _ => throw ClickHouseAnalysisException(s"parse full engine failed. $engine_full")
    }
}
