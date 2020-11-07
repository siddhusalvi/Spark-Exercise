


object pairs{
  def isPrime(i: Int) = (2 until n) forall(n % _ != 0)
  val n = 7
  (1 until n)flatMap (i =>
    (1 until i)map(j=>(i,j))) filter(pair => (
  isPrime(pair._1 + pair._2)))

  case class Person(name:String,age:Int)

  def getPersons(persons: Array[Person]) =
  for (p <- persons if p.age > 20) yield  p.name

  def scalaProducts (xs:List[Double],ys:List[Double]) =
    (for((x,y)<- xs zip ys)yield x*y).sum
}






