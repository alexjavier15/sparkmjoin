package org.epfl.mjoin

import java.io.PrintStream
import java.net.{ServerSocket, Socket}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable
import scala.io.BufferedSource
import scala.reflect.io.File



object RelationTest {
  var dataPath = ""

  def main(args: Array[String]) {

    dataPath = args(0);
    val data_size1 = args(1)
    val data_size2 = args(2)
    val data_size3 = args(3)
    val mjoin = args(4)
    val sampling = args(5)
    val numPart = args(6).toLong


    try {



      val confsString: Map[String, String] = Map(
        ("spark.sql.mjoin", mjoin),
        ("spark.sql.mjoin.sampling", sampling)
      )
      val confsLong: Map[String, Long] = Map(
        ("spark.sql.shuffle.partitions", numPart)
      )


      val conf = new SparkConf()

        .setAppName("TestMaster")

      val sc = SparkContext.getOrCreate(conf)

      val sqlContext = new SQLContext(sc)

      confsString.foreach(conf =>
        sqlContext.setConf(conf._1, conf._2)

      )
      confsLong.foreach(conf =>
        sqlContext.setConf(conf._1, conf._2.toString)

      )

      if (File("tmp.txt").exists)
        File("tmp.txt").delete()

      val start = System.currentTimeMillis()

      val dfC = sqlContext.read
        .format("pf")
        .option("header", "false")
        .load(dataPath + "/C" + data_size1 + ".pf")
      val dfD = sqlContext.read
        .format("pf")
        .option("header", "false")
        .load(dataPath + "/D" + data_size2 + ".pf")
      val dfE = sqlContext.read
        .format("pf")
        .option("header", "false")
        .load(dataPath + "/E" + data_size3 + ".pf")

      dfC.registerTempTable("C")
      dfD.registerTempTable("D")
      dfE.registerTempTable("E")
      val iter = sqlContext.sql("SELECT count(*) FROM E,D,C WHERE C.id = D1 AND D.id = E1").show()



      val end = System.currentTimeMillis()
      val duration = end - start
      println("*************Duration : " + duration + "**************")
    }
    catch {
      case e: Exception =>
        e.printStackTrace(System.err)
    }
    finally {
      //   t1.stop = true
    }


  }
}