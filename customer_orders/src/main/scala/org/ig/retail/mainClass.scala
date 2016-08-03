package org.ig.retail

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object mainClass {
  
  def main(args: Array[String]): Unit = {
     
  // #############################do sqlContext after this
    
  val sparkConf = new SparkConf().setAppName("CustOrdersApp")
 // .setMaster("local[*]")  //local
  val sparkCont = new SparkContext(sparkConf)

  
  val RDDorders = sparkCont.textFile(args(0)).map( x => x.split(",")).map(x => x(0) -> (x(1),x(2)))
    
  
val RDDCust = sparkCont.textFile(args(1)).map( x => x.split(",")).map{ x => x(2) -> (x(1),x(3))}
  
val RDDjoin =   RDDorders.join(RDDCust).saveAsTextFile(args(2))
  
  
  
  }
  
}