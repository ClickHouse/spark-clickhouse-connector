package xenon.clickhouse.spec

import xenon.clickhouse.exception.ClickHouseClientException
import xenon.clickhouse.parse.{AstVisitor, SQLParser}

object TableEngineUtils {
  private val parser = new SQLParser(new AstVisitor)

  def resolveTableEngine(tableSpec: TableSpec): TableEngineSpec = synchronized(
    parser.parseEngineClause(tableSpec.engine_full)
  )

  def resolveTableCluster(distributedEngineSpec: DistributedEngineSpec, clusterSpecs: Seq[ClusterSpec]): ClusterSpec =
    clusterSpecs.find(_.name == distributedEngineSpec.cluster)
      .getOrElse(throw ClickHouseClientException(s"Unknown cluster: ${distributedEngineSpec.cluster}"))
}
