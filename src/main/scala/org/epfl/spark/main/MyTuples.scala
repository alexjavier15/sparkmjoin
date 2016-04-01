package org.epfl.spark.main

/**
  * Created by alex on 14.03.16.
  */
case class MyTuple[+T1 <: Int, +T2 <: Int](val _1: T1,
                                            val _2: T2)  {
  override def toString(): String = _1+"|"+_2
}
