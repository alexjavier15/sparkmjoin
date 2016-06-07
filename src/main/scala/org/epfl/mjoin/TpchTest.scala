package org.epfl.mjoin

import java.io.FileWriter

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.epfl.mjoin.tpch.TpchQuery

import scala.reflect.io.File

/**
  * Created by alex on 04.05.16.
  */
object TpchTest {

    def main(args: Array[String]) {

      val dataPath=args(0);
      val queryPath = args(1)
      //val data_size1 = args(1)
      //val data_size2 = args(2)
      //val data_size3 = args(3)
      val query = args(2)
      val variant = args(3)
      val mjoin = args(4)
      val isExplain = args(5) == "true"
      val sampling = args(6)
      val numPart = args(7).toLong
 
      try {



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
        sqlContext.sparkContext.hadoopConfiguration.setInt("dfs.blocksize", 1024*1024*64)
	sqlContext.sparkContext.hadoopConfiguration.setInt("fs.swift.blocksize", 1024*1024*64)
        sqlContext.sparkContext.hadoopConfiguration.setInt("fs.local.block.size", 1024*1024*64)




        confsString.foreach( conf =>
          sqlContext.setConf(conf._1,conf._2)

        )
        confsLong.foreach( conf =>
          sqlContext.setConf(conf._1,conf._2.toString)

        )

        if(File("tmp.txt").exists)

          File("tmp.txt").delete()

        val  start = System.currentTimeMillis()
	      LogManager.getRootLogger.setLevel(Level.FATAL)

        initRelations(dataPath,sqlContext)
        if(isExplain)
          sqlContext.sql(TpchQuery.getQuery(queryPath,query,variant)).explain(true)
        else
        sqlContext.sql(TpchQuery.getQuery(queryPath,query,variant)).show()



        val end = System.currentTimeMillis()
        val duration = end-start
        println("*************Duration : "+ duration +"**************")


        val fw = new FileWriter("/home_local/rivas/sparkmjoin/results.txt", true)
        val mode = mjoin match{
          case  "true" => "NORMAL"
          case  _ => "OPTIMIZED"
        }
        val result = Seq[String](mode,query,variant,duration.toString).mkString(",")
        fw.write(result)
        fw.close()


      }
      catch {
        case e : Exception =>
          e.printStackTrace(System.err)
      }
      finally {



      }




    }

  def initRelations(dataPath: String, sqlContext : SQLContext ) : Unit ={

   TpchQuery.relations.foreach{
     name =>
     loadRelationAs(dataPath+"/"+name,name,sqlContext)

   }
  }


  def loadRelationAs(path : String,name :String , sqlContext : SQLContext): Unit = {
	println(path)
    val relation = sqlContext.read
      .format("pf")
      .option("header", "false")
      .option("charset","UTF-8")
      .option("delimiter","|")
      .load(path+".pf")

    relation.registerTempTable(name)
  }
}

