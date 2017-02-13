/**
  * Created by ruiwang on 2/11/17.
  *
  * Spark Doc
  * http://spark.apache.org/docs/latest/quick-start.html
  * http://spark.apache.org/docs/latest/programming-guide.html
  *
  * Spark Scala API
  *   https://spark.apache.org/docs/latest/api/scala/#package
*/

import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[2]")  // not needed if specify in spark-submit
      .setAppName("Simple Application")
    val sc = new SparkContext(conf)


    //  ----------- RDD  -----------
    // create RDD option 1: create distributed datasets from any storage source
    // supported by Hadoop, including your local file system, HDFS,
    // Cassandra, HBase, Amazon S3, etc.
    // Spark supports text files, SequenceFiles, and any other Hadoop InputFormat.
    val logFile = "data/input/*.txt"  // path can also be a dir
    // specify partition number
    // cache in cluster-wide cache, with different storage levels
    val logData = sc.textFile(logFile, 2).persist()
    // can also manually remove an RDD
    // logData.unpersist()

    // collect entire RDD to driver program, may OOM
    logData.collect().foreach(println)
    // take first 10 to driver program
    logData.take(10).foreach(println)

    val wordCounts = logData.flatMap(line => line.split(" "))
      .map(k => (k, 1))
      .reduceByKey((a,b) => a+b)
      .sortByKey()
    println("wordCounts")
    wordCounts.take(10).foreach(println)
    // will fail if dir already exist
//    wordCounts.saveAsTextFile("data/output/word_counts")

    // Passing functions
    // option 1: anonymous function
    val numAs = logData.filter(line => line.contains("a")).count()
    // option 2: static methods in a global singleton object
    val numBs = logData.filter(MyFuncs.func1).count()
    println("\nLines with a: %s, Lines with b: %s \n".format(numAs, numBs))

    // create RDD option 2: parallelizing an existing collection in your driver program
    val data = Array(1, 2, 3)
    val distData = sc.parallelize(data)
    distData.take(10).foreach(println)


    //  ----------- Shared variables -----------

    // 1. broadcast: read-only variable cached on each machine
    val broadcastVar = sc.broadcast(2)
    distData.foreach(x => println("x = %s, broadcastVar = %s".format(x, broadcastVar.value)))
    println("broadcast %s".format(broadcastVar.value))

    // 2. accumulator: long or double type, or create own type
    // task can add, but can not read value; only driver can read
    val accum = sc.longAccumulator("My longAccumulator")
//    val accum = sc.doubleAccumulator("My doubleAccumulator")
    distData.foreach(x => accum.add(x))
    println("accum %s".format(accum.value))

  }
}

object MyFuncs {
  def func1(s: String): Boolean = {
    return s.contains("b")
  }
}