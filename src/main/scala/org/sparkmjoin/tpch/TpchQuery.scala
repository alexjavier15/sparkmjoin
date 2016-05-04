package org.sparkmjoin.tpch
import scala.io.Source.fromFile
/**
  * Created by alex on 04.05.16.
  */
object TpchQuery {
val queryPath : String = "queries"
val relations : Seq[String] = Seq("region","nation","part","supplier","customer","partsupp","orders","lineitem")

def getQuery(queryNum : String, variant: String): String ={

 val queryIter = fromFile(queryPath+"/"+queryNum+"_"+variant).getLines()

   queryIter.mkString

}





}

