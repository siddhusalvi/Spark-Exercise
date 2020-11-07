def isort(xs:List[Int]):List[Int] = xs match {
  case List() =>List()
  case y ::ys => insert(y, isort(ys))
}

def insert(x:Int,xs:List[Int]): List[Int] = xs match {
  case List() => ???
  case y :: ys => ???
}

def last[T](xs:List[T]): T = xs match{
  case List() => throw  new Error("last of empty lsit")
  case List(x) => x
  case y :: ys => last(ys)
}

def init[T](xs:List[T]): List[T] = xs match {
  case List() => throw new Error("init of empty list")
}

def concat[T](xs: List[T], ys:List[T]) = xs match {
  case  List() => ys
  case z::zs => z::concat(zs,ys)
}

def reverse[T](xs: List[T]):List[T] = xs match {
  case List() =>xs
  case y :: ys => reverse(ys) ++ List(y)
}

object  week5{
  val fruit = List("apples","banana","mango")
  val nums = List(1,2,3)

  def removeAt(n:Int,xs:List[Int])=(xs take n) ::: (xs drop n +1)
}

def msort(xs: List[Int]): List[Int] = {
  val n = xs.length/2
  if (n == 0) xs
  else{
    def merge(xs:List[Int],ys:List[Int]) = ???
    val (fst,snd) = xs splitAt n
    merge(msort(fst),msort(snd))
  }
}

def merge(xs: List[Int], ys:List[Int]) = xs match {
  case Nil => ys
  case x :: xs1 =>
    ys match {
      case Nil => xs
      case y :: ys1 =>
        if (x < y) x :: merge(xs1, ys)
        else y :: merge(xs, ys1)
    }
}

//object mergersort{
//  def msort(xs:List[Int],ys:List[Int]):List[Int] = (xs,ys)match {
//    case (Nil,ys) =>ys
//    case (xs,Nil) =>xs
//    case (x::xs1,y::ys1)=>
//      if(x<y)x::merge(xs1,ys)
//      else y:: merge(xs,ys1)
//  }
//  val (fst,snd) = xs splitAt n
//  merge(msort(fst),msort(snd))
//
//}






