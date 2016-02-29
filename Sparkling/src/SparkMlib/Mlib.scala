package SparkMlib

import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.Rating

object Mlib {
  
  def main(args: Array[String]){
    
    //val vec:Vector=Vectors.dense(1.0,2.0)
    val vec = Vectors.dense(10.0,20.0)
     val vec1 =Vectors.dense(3.0,4.0,5.0,6.0)
    
    println(vec)
    println(vec1)
    
    val sv1 = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
    val sv2 = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
    
    println(sv1)
    println(sv2)
    
    val lab1 = LabeledPoint(1.0,Vectors.dense(1.0, 2.0,3.0))
    val lab2 = LabeledPoint(1.0,vec)
    
    println(lab1)
    println(lab2)
    
    val rat = Rating(10,20,30)
    
    println(rat)
    
    
  }
  
}