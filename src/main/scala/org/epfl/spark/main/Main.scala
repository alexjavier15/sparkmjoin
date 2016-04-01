package org.epfl.spark.main

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.PairRDDFunctions


object Main {

  

  def main(args: Array[String]){

    if (args.length < 2)
      throw new RuntimeException("Invalid number of arguments")
    val sc = new SparkContext(new SparkConf().setAppName("Project_1.1").setMaster("local"))

    val input_path = args(0)
    val output_path = args(1)

    val comment_pattern = ".*special.*requests.*"

    val customer_file = sc.textFile(input_path + "/customer.tbl")
    val c_custkeys = customer_file.map(_.split('|'))
      .map(p => (p(0).trim.toInt, 0)).cache()

    val orders_file = sc.textFile(input_path + "/orders.tbl")
    val o_custkeys = orders_file.map(_.split('|'))
      .map(o => ( o(1).trim.toInt , o(8).trim)).filter( o => !o._2.matches(comment_pattern))
      .map(o => (o._1, 1))

    val grouped = c_custkeys
      .union(o_custkeys)
      .reduceByKey( _ + _)
      .map(j => (j._2, 1))
      .reduceByKey(_+_)
      .sortBy(s => s._1, true).map(t => MyTuple(t._1, t._2))
     .saveAsTextFile(output_path+ "/out")




  }



}