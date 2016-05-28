package org.epfl.mjoin.tpch
import java.io.{FileOutputStream, OutputStream, PrintStream}
import java.net.{ServerSocket, Socket}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.commons.net.ftp
import org.apache.commons.net.ftp.FTPClient
import org.epfl.mjoin.swift.TpchSwift

import scala.collection.mutable
import scala.io.BufferedSource
import scala.io.Source._
import scala.reflect.io.File



object TpchSwiftTest {
  var dataPath = ""
  def main(args: Array[String]) {

    dataPath=args(0);
    val t1  = new MyThread
    val t2  = new SparkMaster
    try {
      val server = (new Thread(t1))
      val spark_master = (new Thread(t2))
      spark_master.setDaemon(false)
      spark_master.start
      server.setDaemon(false)
      server.start

      Thread.sleep(1000)
      val swift1 = new TpchSwift
      swift1.listContainers()

      System.exit(0)

      val confs = Map(
        ("spark.sql.codegen.wholeStageEnabled", "false"),
        ("spark.sql.codegen", "false"),
        ("spark.sql.codegen.WholeStage", "false"),
        ("spark.sql.mjoin", "true"),
        ("spark.sql.mjoin.sampling","true"),
        ("spark.sql.join.preferSortMergeJoin", "false"),
        ("spark.sql.autoBroadcastJoinThreshold", "1"),
        ("spark.sql.shuffle.partitions","20"),
        ("spark.sql.IteratedHashJoin", "true")
      )

      val sc = SparkContext.getOrCreate()
      val sqlContext = new SQLContext(sc)
      sqlContext.sparkContext.hadoopConfiguration.setInt("dfs.blocksize", 70*1024*1024)
      sqlContext.sparkContext.hadoopConfiguration.setInt("fs.local.block.size", 70*1024*1024)
      sqlContext.sparkContext.hadoopConfiguration.set("fs.swift.service.PROVIDER.auth.url", "http://172.16.1.29:8080/auth/v1.0")
      sqlContext.sparkContext.hadoopConfiguration.set("fs.swift.impl", "org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem")

      /*     sc.submitJob(adressses,(iterator : Iterator[Int]) => {
             while(iterator.hasNext){
               val next = iterator.next
               download(next)

             }

           }, 0 until adressses.partitions.size, resultHandler, () => Unit)

           sc.submitJob(adressses2,(iterator : Iterator[Int]) => {
             while(iterator.hasNext){
               val next = iterator.next
               download(next)

             }

           }, 0 until adressses2.partitions.size, resultHandler, () => Unit)*/

      confs.foreach( conf =>
        sqlContext.setConf(conf._1,conf._2)

      )




      // Importing the SQL context gives access to all the SQL functions and implicit conversions.
      import sqlContext.implicits._
      // val df = sc.parallelize((1 to 100).map(i => Record(i, s"val_$i"))).toDF()

      val dfC = sqlContext.read
        .format("pf")
        .option("header", "false")
        .load(dataPath+"/C100k.pf")
      val dfD = sqlContext.read
        .format("pf")
        .option("header", "false")//true for csv
        //.option("inferSchema", "true")
        .load(dataPath+"/D400k.pf")
      val dfE = sqlContext.read
        .format("pf")
        .option("header", "false")//true for csv
        //.option("inferSchema", "true")
        .load(dataPath+"/E400k.pf")
      val dfF = sqlContext.read
        .format("pf")
        .option("header", "false")//true for csv
        //.option("inferSchema", "true")
        .load(dataPath+"/E100k.pf")
      dfC.registerTempTable("C")
      dfD.registerTempTable("D")
      dfE.registerTempTable("E")
      dfF.registerTempTable("F")
      val start = System.currentTimeMillis()
      sqlContext.sql("SELECT count(*) FROM E,D,C,F WHERE C.id = D1 AND D1 = E.E1 AND D.id = F.E1  ").show()
      // sqlContext.sql("SELECT count(*) FROM E,D,C WHERE C.id = D1 AND D.id = E1").show()
      // sqlContext.sql("SELECT * FROM E,D,C WHERE C1 = D1 AND D1 = E1").show(100000)
      val end = System.currentTimeMillis()
      val duration= end - start
      println("*********Duration : "+ duration +"***********")


    }
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
  var sc : SparkContext  = init

  def init() : SparkContext ={
    val event_dir = File("/tmp/spark-events")
    if(!event_dir.exists)
      event_dir.createDirectory()


    val conf = new SparkConf().setMaster("local[*]")
      // .set("spark.metrics.conf", LocalSparkTest.dataPath+"/metrics.txt")
      //.        setSparkHome("/home/alex/spark1").
      .set("spark.eventLog.enabled","true")
      //set("spark.cores.max","4").
      //      set("spark.executor.instances", "3")
      .setAppName("TestMaster")
      .set("spark.scheduler.mode", "FAIR")

    SparkContext.getOrCreate(conf)

  }

  def run {
    import sys.process._





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
        out.println(TpchSwiftTest.dataPath +"/C3.csv"+System.lineSeparator()+TpchSwiftTest.dataPath +"/C2.csv")
        out.flush()
        out.close()
        println("Replying request !")

      }
      case DISCONNECT => socket.close()
    }

  }




}