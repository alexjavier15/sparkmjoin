package org.sparkmjoin

import java.io.PrintStream
import java.net.{ServerSocket, Socket}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable
import scala.io.BufferedSource



object RelationTest {
  var dataPath = ""
  def main(args: Array[String]) {

    dataPath=args(0);
    val t1  = new MyThread
    val t2  = new SparkMaster
    try {
      val server = (new Thread(t1))
//      val spark_master = (new Thread(t2))
  //    spark_master.setDaemon(false)
   //   spark_master.start
      server.setDaemon(false)
      server.start

      Thread.sleep(1000)




      val confs = Map(
        ("spark.sql.codegen.wholeStageEnabled", "false"),
        ("spark.sql.codegen", "false"),
        ("spark.sql.codegen.WholeStage", "false"),
        ("spark.sql.mjoin", "true"),
        ("spark.sql.join.preferSortMergeJoin", "false")
        ,  ("spark.sql.autoBroadcastJoinThreshold", "1")
        ,("spark.sql.IteratedHashJoin", "true")
      )

      val sc = new SparkContext(new SparkConf().setAppName("RelationTest"))

   //   val conf = new SparkConf().setMaster("spark://alex-HP:7077")
     //   .setAppName("TestMaster").set("spark.driver.memory","512m")
       // .set("spark.executor.memory","512m")
        //.set("spark.default.parallelism","1")
      //val sc = SparkContext.getOrCreate(conf)

      val sqlContext = new SQLContext(sc)




      confs.foreach( conf =>
      sqlContext.setConf(conf._1,conf._2)

      )


      // Importing the SQL context gives access to all the SQL functions and implicit conversions.
      import sqlContext.implicits._

      // val df = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i"))).toDF()
      val dfC = sqlContext.read
        .format("pf")
        .option("header", "false")
        .load(dataPath+"/MyFile.pf")
      val dfD = sqlContext.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(dataPath+"/D.csv")
      val dfE = sqlContext.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(dataPath+"/E.csv")

      dfC.registerTempTable("C")
      dfD.registerTempTable("D")
      dfE.registerTempTable("E")
      sqlContext.sql("SELECT count(*) FROM C,E,D WHERE C1 = D1 AND D1 = E1").show}
    catch {
      case e : Exception =>
          e.printStackTrace(System.err)
    }
    finally {
      t1.stop = true
    }




  }
}
// scalastyle:on println
class MyThread extends Runnable {

  var stop : Boolean = false
  val workers : mutable.Seq[WorkerThread] = mutable.Seq[WorkerThread]()
  def run {
    var i = 0
    val server = new ServerSocket(9999)
    while (!stop) {


      val s = server.accept()
      println("Received : " + i + " requests  from :" + s.getRemoteSocketAddress)
      val worker = (new WorkerThread(s))
      workers:+worker
      (new Thread(worker)).start
      i+=1

    }
    workers.foreach(worker => worker.stop =true)
    server.close()
  }

}

// scalastyle:on println
class SparkMaster extends Runnable {

  var stop : Boolean = false
  var started : Boolean = false
  var sc : SparkContext  = null

  def run {
    import sys.process._

      val conf = new SparkConf().setMaster("spark://alex-HP:7077")
        //.        setSparkHome("/home/alex/spark1").
      //  set("spark.default.parallelism","2").
        //set("spark.cores.max","4").
  //      set("spark.executor.instances", "3")
        .setAppName("TestMaster")

        sc = SparkContext.getOrCreate(conf)


    println(sc.master)

   // "bash /home/alex/spark1/sbin/start-slave.sh http://localhost:7077".!
    while (!stop) {



    }
    started =false
    sc.stop()
  }

}

class WorkerThread(socket : Socket) extends Runnable {
  var stop : Boolean = false


  def run {
   try  {
     val in = new BufferedSource(socket.getInputStream())
     val out = new PrintStream(socket.getOutputStream())
    while (!stop && !socket.isClosed ) {

        val lines = in.getLines()

        while (lines.hasNext) {

          CommadProcessor.processMessage(socket,lines.next,out)
        }

    }
     println("closing Worker")
     out.close()
     in.close()

   }
   catch
     {
       case e: Exception => println(e.getMessage)
     }
    finally {
      if(!socket.isClosed)
          socket.close()


    }
  }

}

object  CommadProcessor{
  final val NEXT_CHUNK = "NEXT"
  final val DISCONNECT = "END"

  def processMessage(socket : Socket, message : String, out : PrintStream) : Unit ={
      val command = message.split(":")(0)
    println("command " + command)
      command match {

        case NEXT_CHUNK => {
          println("GOT " + message)
          out.println(RelationTest.dataPath +"/C3.csv"+System.lineSeparator()+RelationTest.dataPath +"/C2.csv")
          out.flush()
          out.close()
          println("Replying request !")

        }
        case DISCONNECT => socket.close()
        }

      }




}