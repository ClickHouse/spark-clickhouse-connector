package xenon.clickhouse.spec

import org.scalatest.funsuite.AnyFunSuite

class NodeSpecSuite extends AnyFunSuite with NodeSpecHelper {

  test("nodes should be sorted") {
    assert(shard_s1.nodes.map(_.host) === Array("s1r1", "s1r2"))
    assert(shard_s2.nodes.map(_.host) === Array("s2r1", "s2r2"))

    assert(cluster.nodes.map(_.host) === Array("s1r1", "s1r2", "s2r1", "s2r2"))
    assert(cluster.nodes.map(_.host) === Array("s1r1", "s1r2", "s2r1", "s2r2"))
  }
}
