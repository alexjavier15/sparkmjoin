

val a  =List(2,3,2)
val seq = Seq(3)
Seq.iterate(Seq(1),seq.head)(x=> Seq(x.head+1))
def permuteWithoutOrdering ( seq : List[Int]):Seq[Seq[Int]] ={

  seq match {

    case x::Nil => Seq.iterate(Seq(1),seq.head)(x=> Seq(x.head+1))
    case _ =>
      for ( y<- Seq.iterate(1,seq.head)(y=> y+1); x <-permuteWithoutOrdering(seq.tail))
        yield(y+: x)
  }


}

val mp = Map ((1,1)-> 2 ,(1,2)-> 2 ,(3,2) ->5)

val g = mp.minBy(_._2)
val c = permuteWithoutOrdering(a)