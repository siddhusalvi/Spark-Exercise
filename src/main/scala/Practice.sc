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
  case y :: ys => reverse(ys) :: List(y)
}