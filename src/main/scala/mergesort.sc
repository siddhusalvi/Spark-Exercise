

object mergesort{
  //Using lt (less than function)
  def msort[T](xs:List[T])(lt:(T,T) => Boolean):List[T] = {
    val n = xs.length / 2
    if (n == 0) {
      xs
    }
    else {
      def merge(xs: List[T], ys: List[T]): List[T] = (xs, ys) match {
        case (Nil, ys) => ys
        case (xs, Nil) => xs
        case (x :: xs1, y :: ys1) =>
          if (lt(x, y)) x :: merge(xs1, ys)
          else y :: merge(xs, ys1)
      }

      val (fst, snd) = xs splitAt n
      merge(msort(fst)(lt), msort(snd)(lt))
    }
  }
    //Using Ordering
  def msort1[T](xs:List[T])(implicit ord: Ordering[T]):List[T] = {
      val n = xs.length / 2
      if(n == 0){
        xs
      }
      else{
        def merge1(xs:List[T],ys:List[T]):List[T] = (xs,ys) match {
          case (Nil,ys) => ys
          case (xs,Nil) => xs
          case (x :: xs1,y::ys1)=>
            if(ord.lt(x,y)) x ::merge1(xs1,ys)
            else y::merge1(xs,ys1)
        }
        val (fst,snd) = xs splitAt n
        merge1(msort1(fst),msort1(snd))
      }


  }

  def squareList(xs:List[Int]):List[Int] = xs match {
    case Nil => Nil
    case y::ys=>y * y ::squareList(ys)
  }


    val nums = List(2,4,-7,1,0)
    msort(nums)((x:Int,y:Int)=> x < y)
    msort1(nums)

    val fruits = List("apple","pineapple","orange","banana")
    msort(fruits)((x:String,y:String)=>x.compareTo(y)<0)
    msort1(fruits)



}