import org.json4s.scalap.Error

/*
abstract class List[T]{
  def filter(p:T=>Boolean):List[T] = this match{
    case Nil => this
    case x::xs => if (p(x)) x::xs.filter else xs.filter(p)
  }
}
*/
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
  //Write function pack that pack consecutive duplicates
  def pack[T](xs:List[T]):List[List[T]]  = xs match{
    case Nil => Nil
    case x::xs =>{
      val (first,rest) = xs span (y => y == x)
      first::pack(rest)
    }
  }

  //output number of time consecutive same element
  def encode[T](xs: List[T]):List[(T,Int)] = {
    pack(xs) map(ys => (ys.head,ys.length))
  }

  //sum
  def sum(xs:List[Int]):Int=xs match {
    case Nil => 0
    case y::ys => y + sum(ys)
  }

  //sum and product using foldLeft

  def add(xs:List[Int]) = (xs foldLeft 0)(_ + _)
  def multiply(xs:List[Int]) = (xs foldLeft 1)(_ * _)


  //map like function
  def squareList(xs:List[Int]):List[Int] = xs match {
    case Nil => Nil
    case y::ys=>y * y ::squareList(ys)


  }

  def conCat[T](xs:List[T],ys:List[T])=xs match {
    case List() => ys
    case x::xs1 => x::conCat(xs1,ys)
  }

  /*
  def reduceRight(op:(T,T) => T):T = this match {
    case Nil => throw  Error("Nil.reduceRight")
    case x :: Nil => x
    case x :: xs => op(x,(xs reduceRight z)(op))
  }
  */

    val nums = List(2,4,-7,1,0)
    msort(nums)((x:Int,y:Int)=> x < y)
    msort1(nums)

    val fruits = List("apple","pineapple","orange","banana")
    msort(fruits)((x:String,y:String)=>x.compareTo(y)<0)
    msort1(fruits)



  //filter
  //filterNot
  nums filterNot (x => x > 0)
  //partition
  nums partition (x => x > 0)
  //takewhile
  nums takeWhile  (x => x > 0)
  //dropWhile
  nums dropWhile (x => x > 0)
  //span
  nums span (x => x > 0)

  val data = List('a','a','a','b','b','c','c')

  pack(data)

  encode(data)

}