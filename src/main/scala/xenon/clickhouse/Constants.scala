package xenon.clickhouse

object Constants {
  // format: off
  //////////////////////////////////////////////////////////
  //////// clickhouse datasource catalog properties ////////
  //////////////////////////////////////////////////////////
  final val CATALOG_PROP_HOST     = "host"
  final val CATALOG_PROP_PORT     = "port"
  final val CATALOG_PROP_USER     = "user"
  final val CATALOG_PROP_PASSWORD = "password"
  final val CATALOG_PROP_DATABASE = "database"
  final val CATALOG_PROP_TZ       = "timezone" // server(default), client, UTC+3, Asia/Shanghai, etc.
  final val CATALOG_PROP_WRITE_BATCH_SIZE = "write.batch-size" // default 1000
  final val CATALOG_PROP_DIST_WRITE_USE_CLUSTER_NODES = "distributed.write.use-cluster-nodes" // true(default), false
  final val CATALOG_PROP_DIST_READ_USE_CLUSTER_NODES  = "distributed.read.use-cluster-nodes"  // true(default), false
  final val CATALOG_PROP_DIST_WRITE_CONVERT_TO_LOCAL  = "distributed.write.convert-to-local"  // true, false(default)
  final val CATALOG_PROP_DIST_READ_CONVERT_TO_LOCAL   = "distributed.read.convert-to-local"   // true, false(default)

  //////////////////////////////////////////////////////////
  ////////// clickhouse datasource read properties /////////
  //////////////////////////////////////////////////////////

  //////////////////////////////////////////////////////////
  ///////// clickhouse datasource write properties /////////
  //////////////////////////////////////////////////////////
  // format: on
}
