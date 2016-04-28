package org.sparkmjoin

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

    dataPath=args(0);
    val data_size1 = args(1)
    val data_size2 = args(2)
    val data_size3 = args(3)
    val mjoin = args(4)
    val sampling = args(5)
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




      val confs = Map(
        ("spark.sql.codegen.wholeStageEnabled", "false"),
        ("spark.sql.codegen", "false"),
        ("spark.sql.codegen.WholeStage", "false"),
        ("spark.sql.mjoin", mjoin),
        ("spark.sql.mjoin.sampling",sampling),
        ("spark.sql.join.preferSortMergeJoin", "false"),
        ("spark.sql.autoBroadcastJoinThreshold", "1"),
        ("spark.sql.IteratedHashJoin", "true")
      )



     val conf = new SparkConf()
        //.setMaster("spark://alex-HP:7077")
        // val conf = new SparkConf().setMaster("local[*]")
        //.        setSparkHome("/home/alex/spark1").
        //  set("spark.default.parallelism","2").
        //set("spark.cores.max","4").
        //      set("spark.executor.instances", "3")
        .setAppName("TestMaster")

    //  val sc = t2.sc

      //   .setAppName("TestMaster").set("spark.driver.memory","512m")
       // .set("spark.executor.memory","512m")
        //.set("spark.default.parallelism","1")
      val sc = SparkContext.getOrCreate(conf)

      val sqlContext = new SQLContext(sc)




      confs.foreach( conf =>
      sqlContext.setConf(conf._1,conf._2)

      )
      if(File("tmp.txt").exists)
        File("tmp.txt").delete()

      // Importing the SQL context gives access to all the SQL functions and implicit conversions.
      import sqlContext.implicits._
      val  start = System.currentTimeMillis()
      // val df = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i"))).toDF()
      val dfC = sqlContext.read
        .format("pf")
        .option("header", "false")
        .load(dataPath+"/C"+data_size1+".pf")
      val dfD = sqlContext.read
        .format("pf")
        .option("header", "false")
        .load(dataPath+"/D"+data_size2+".pf")
      val dfE = sqlContext.read
        .format("pf")
        .option("header", "false")
        .load(dataPath+"/E"+data_size3+".pf")

      dfC.registerTempTable("C")
      dfD.registerTempTable("D")
      dfE.registerTempTable("E")
      sqlContext.sql("SELECT * FROM E,D,C WHERE C.id = D1 AND D1 = E1").rdd.saveAsTextFile("tmp.txt")


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
  var sc : SparkContext  = init


  def init : SparkContext = {
    val conf = new SparkConf().setMaster("local[4]")
      // val conf = new SparkConf().setMaster("local[*]")
      //.        setSparkHome("/home/alex/spark1").
      //  set("spark.default.parallelism","2").
      //set("spark.cores.max","4").
      //      set("spark.executor.instances", "3")
      .setAppName("TestMaster")

    SparkContext.getOrCreate(conf)


  }
  def run {
    import sys.process._




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