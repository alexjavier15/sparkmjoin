package org.epfl.mjoin.tpch

import scala.io.Source.fromFile
/**
  * Created by alex on 04.05.16.
  */
object TpchQuery {
val relations : Seq[String] = Seq("region","nation","part","supplier","customer","partsupp","orders","lineitem")

def getQuery(queryPath: String , queryNum : String, variant: String): String ={

 val queryIter = fromFile(queryPath+"/"+queryNum+"_"+variant+".sql").getLines()

   val query = queryIter.mkString(" ")
  println(query)

  query.replaceAll(";", "")

}





}

