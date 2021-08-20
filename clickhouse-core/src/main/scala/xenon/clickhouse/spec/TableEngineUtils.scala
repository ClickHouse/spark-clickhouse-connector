package xenon.clickhouse.spec

import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.parse.{AstVisitor, SQLParser}

object TableEngineUtils {
  private val parser = new SQLParser(new AstVisitor)

  def resolveTableEngine(tableSpec: TableSpec): TableEngineSpecV2 = synchronized(
    parser.parseEngineClause(tableSpec.engine_full)
  )

  def resolveTableCluster(distributedEngineSpec: DistributedEngineSpecV2, clusterSpecs: Seq[ClusterSpec]): ClusterSpec =
    clusterSpecs.find(_.name == distributedEngineSpec.cluster)
      .getOrElse(throw ClickHouseClientException(s"Unknown cluster: ${distributedEngineSpec.cluster}"))
}
