package de.dlh.smile.axdelivery.data

abstract class Score[A] {
  val value: A
  val coefficient:Double
  val result: Double = operation(value, coefficient)
  def operation: (A, Double) => Double

}

case class FixedStringScore(val value: String, val coefficient: Double) extends Score[String] {
  override def operation = (value: String, coefficient: Double) => coefficient
}

case class DynamicIntScore(val value: Int, val coefficient: Double) extends Score[Int] {
  override def operation = (value: Int, coefficient: Double) => value * coefficient
}

case class LeisureScore(scores: List[Score[_ >: Int with String]]){
  val result = 1/(1+math.exp(-scores.foldLeft(-2.416664151){(data,element) => data + element.result}))
}
