package org.sparkmjoin

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.sparkmjoin.tpch.TpchQuery

import scala.reflect.io.File

/**
  * Created by alex on 04.05.16.
  */
object TpchTest {

    var dataPath = ""
    def main(args: Array[String]) {

      dataPath=args(0);
      //val data_size1 = args(1)
      //val data_size2 = args(2)
      //val data_size3 = args(3)
      val query = args(1)
      val variant = args(2)
      val mjoin = args(3)
      val sampling = args(4)
      val numPart = args(5).toLong
      val t1  = new MyThread
      //al t2  = new SparkMaster
      try {
        val server = (new Thread(t1))
        // val spark_master = (new Thread(t2))
        //spark_master.setDaemon(false)
        //spark_master.start
        server.setDaemon(false)
        server.start

        Thread.sleep(1000)




        val confsString : Map[String,String] = Map(
          ("spark.sql.mjoin", mjoin),
          ("spark.sql.mjoin.sampling",sampling)
        )
        val confsLong  : Map[String,Long] = Map(
          ("spark.sql.shuffle.partitions",numPart)
        )


        val conf = new SparkConf()

          .setAppName("TestMaster")

        val sc = SparkContext.getOrCreate(conf)

        val sqlContext :SQLContext = new SQLContext(sc)
        sqlContext.sparkContext.hadoopConfiguration.setInt("dfs.block.size", 1024*1024*1024)
        sqlContext.sparkContext.hadoopConfiguration.setInt("fs.local.block.size", 1024*1024*1024)




        confsString.foreach( conf =>
          sqlContext.setConf(conf._1,conf._2)

        )
        confsLong.foreach( conf =>
          sqlContext.setConf(conf._1,conf._2.toString)

        )

        if(File("tmp.txt").exists)
          File("tmp.txt").delete()

        val  start = System.currentTimeMillis()

        initRelations(dataPath,sqlContext)

        val iter = sqlContext.sql(TpchQuery.getQuery(query,variant)).show()



        val end = System.currentTimeMillis()
        val duration = end-start
        println("*************Duration : "+ duration +"**************")
      }
      catch {
        case e : Exception =>
          e.printStackTrace(System.err)
      }
      finally {
        //   t1.stop = true
      }




    }

  def initRelations(dataPath: String, sqlContext : SQLContext ) : Unit ={

   TpchQuery.relations.foreach{
     relName =>
     loadRelationAs(relName,sqlContext)

   }
  }


  def loadRelationAs(name : String, sqlContext : SQLContext): Unit = {

    val relation = sqlContext.read
      .format("pf")
      .option("header", "false")
      .load(dataPath+name+".pf")

    relation.registerTempTable(name)
  }
}

