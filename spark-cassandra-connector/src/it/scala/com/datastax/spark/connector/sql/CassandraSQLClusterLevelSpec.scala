package com.datastax.spark.connector.sql

import scala.concurrent.Future

import com.datastax.spark.connector.SparkCassandraITFlatSpecBase
import com.datastax.spark.connector.cql.{CassandraConnectorConf, CassandraConnector}
import com.datastax.spark.connector.embedded.EmbeddedCassandra._
import org.apache.spark.sql.cassandra._

import com.datastax.spark.connector.rdd.ReadConf

class CassandraSQLClusterLevelSpec extends SparkCassandraITFlatSpecBase {
  useCassandraConfig(Seq("cassandra-default.yaml.template", "cassandra-default.yaml.template"))
  useSparkConf(defaultConf)

  val conn = CassandraConnector(defaultConf)

  val conf2 = defaultConf
    .set("spark.cassandra.connection.host", getHost(1).getHostAddress)
    .set("spark.cassandra.connection.port", getPort(1).toString)
  val conn2 = CassandraConnector(conf2)

  awaitAll(
    Future {
      conn.withSessionDo { session =>
        createKeyspace(session, ks)

        session.execute(s"CREATE TABLE $ks.test1 (a INT PRIMARY KEY, b INT, c INT)")
        session.execute(s"INSERT INTO $ks.test1 (a, b, c) VALUES (1, 1, 1)")
        session.execute(s"INSERT INTO $ks.test1 (a, b, c) VALUES (2, 1, 2)")
        session.execute(s"INSERT INTO $ks.test1 (a, b, c) VALUES (3, 1, 3)")
        session.execute(s"INSERT INTO $ks.test1 (a, b, c) VALUES (4, 1, 4)")
        session.execute(s"INSERT INTO $ks.test1 (a, b, c) VALUES (5, 1, 5)")
      }
    },

    Future {
      conn2.withSessionDo { session =>
        createKeyspace(session, ks)

        awaitAll(
          Future {
            session.execute(s"CREATE TABLE $ks.test2 (a INT PRIMARY KEY, d INT, e INT)")
            session.execute(s"INSERT INTO $ks.test2 (a, d, e) VALUES (8, 1, 8)")
            session.execute(s"INSERT INTO $ks.test2 (a, d, e) VALUES (7, 1, 7)")
            session.execute(s"INSERT INTO $ks.test2 (a, d, e) VALUES (6, 1, 6)")
            session.execute(s"INSERT INTO $ks.test2 (a, d, e) VALUES (4, 1, 4)")
            session.execute(s"INSERT INTO $ks.test2 (a, d, e) VALUES (5, 1, 5)")
          },

          Future {
            session.execute(s"CREATE TABLE $ks.test3 (a INT PRIMARY KEY, d INT, e INT)")
          }
        )
      }
    }
  )

  var cc: CassandraSQLContext = null

  override def beforeAll() {
    cc = new CassandraSQLContext(sc)
    cc.setConf("cluster1/spark.cassandra.connection.host", getHost(0).getHostAddress)
    cc.setConf("cluster1/spark.cassandra.connection.port", getPort(0).toString)
    cc.setConf("cluster2/spark.cassandra.connection.host", getHost(1).getHostAddress)
    cc.setConf("cluster2/spark.cassandra.connection.port", getPort(1).toString)
  }

  it should "allow to join tables from different clusters" in {
    cc.read.cassandraFormat("test1", ks, "cluster1").load().registerTempTable("c1_test1")
    cc.read.cassandraFormat("test2", ks, "cluster2").load().registerTempTable("c2_test2")

    val result = cc.sql(s"SELECT * FROM c1_test1 AS test1 JOIN c2_test2 AS test2 WHERE test1.a = test2.a").collect()
    result should have length 2
  }

  it should "allow to write data to another cluster" in {
    cc.read.cassandraFormat("test1", ks, "cluster1").load().registerTempTable("c1_test1")
    cc.read.cassandraFormat("test3", ks, "cluster2").load().registerTempTable("c2_test3")

    val insert = cc.sql(s"INSERT INTO TABLE c2_test3 SELECT * FROM c1_test1 AS t1").collect()
    val result = cc.sql(s"SELECT * FROM c2_test3 AS test3").collect()
    result should have length 5
  }
}
