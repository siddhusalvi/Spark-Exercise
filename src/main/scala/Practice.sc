def isort(xs:List[Int]):List[Int] = xs match {
  case List() =>List()
  case y ::ys => insert(y, isort(ys))
}

def insert(x:Int,xs:List[Int]): List[Int] = xs match {
  case List() => ???
  case y :: ys => ???
}