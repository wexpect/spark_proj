/*
http://spark.apache.org/docs/latest/quick-start.html
http://spark.apache.org/docs/latest/programming-guide.html
*/

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Simple Application")
    val sc = new SparkContext(conf)

    // Option1: create RDD from file
    val logFile = "data/input/*.txt"
    val logData = sc.textFile(logFile, 2).persist()
    // logData.unpersist()  // manual remove an RDD

    // collect entire RDD to driver program, may OOM
    logData.collect().foreach(println)
    // take first 10 to driver program
    logData.take(10).foreach(println)

    val wordCounts = logData.flatMap(line => line.split(" ")).
      map(k => (k, 1)).reduceByKey((a,b) => a+b).sortByKey()
    println("wordCounts")
    wordCounts.take(10).foreach(println)
//    wordCounts.saveAsTextFile("data/output/word_counts")

    val numAs = logData.filter(line => line.contains("a")).count()
//    val numBs = logData.filter(line => line.contains("b")).count()
    val numBs = logData.filter(MyFuncs.func1).count()
    println("\nLines with a: %s, Lines with b: %s \n".format(numAs, numBs))

    // Option 2: create RDD from seq
    val data = Array(1, 2, 3)
    val distData = sc.parallelize(data)
    distData.take(10).foreach(println)


    // broadcast
    val broadcastVar = sc.broadcast(2)
    distData.foreach(x => println("x = %s, broadcastVar = %s".format(x, broadcastVar.value)))
    println("broadcast %s".format(broadcastVar.value))

    // accumulator
    val accum = sc.longAccumulator("My Accumulator")
    distData.foreach(x => accum.add(x))
    println("accum %s".format(accum.value))

  }
}

object MyFuncs {
  def func1(s: String): Boolean = {
    return s.contains("b")
  }
}