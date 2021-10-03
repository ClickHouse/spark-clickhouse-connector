package xenon.clickhouse.cluster

class ClusterNodesWriteSuite extends BaseClusterWriteSuite {

  override def sparkOptions: Map[String, String] = super.sparkOptions +
    ("spark.clickhouse.write.write.repartitionNum" -> "0") +
    ("spark.clickhouse.write.distributed.useClusterNodes" -> "true") +
    ("spark.clickhouse.write.distributed.convertLocal" -> "false")
}
