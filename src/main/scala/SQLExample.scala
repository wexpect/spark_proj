/**
  * Created by ruiwang on 2/11/17.
  *
  * SQL Doc
  *   http://spark.apache.org/docs/latest/sql-programming-guide.html
  */

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

//import org.apache.spark.sql.{Encoder, Encoders}
//import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

object SQLExample {

  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface
  // Note: case class, by use of which you define the schema of the DataFrame, should be defined outside of the method needing it.
  // http://community.cloudera.com/t5/Advanced-Analytics-Apache-Spark/Spark-Scala-Error-value-toDF-is-not-a-member-of-org-apache-spark/ta-p/39122
  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")  // not needed if specify in spark-submit
      .appName("SQL Example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    // This import is needed to use the $-notation
    import spark.implicits._


    //  ----------- DataFrame  -----------

    val people_json_path = "examples/src/main/resources/people.json"
    val df = spark.read.json(people_json_path)

    df.printSchema()

    df.show()

    df.select("name").show()  // org.apache.spark.sql.DataFrame = [name: string]
    df.select($"name").show()

    // This is WRONG: "cannot resolve '`age1`' given input columns: [age, name]"
    // df.select("name", "age" + 1).show()

    // This is RIGHT: org.apache.spark.sql.DataFrame = [name: string, (age + 1): bigint]
    df.select($"name", $"age" + 1).show()

    df.filter($"age" > 21).show() // org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [age: bigint, name: string]

    df.groupBy("age").count().show()


    // ----------- SQL ------------

    // Register the DataFrame as a SQL temporary view
    // Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

    // Register the DataFrame as a global temporary view, which is
    // shared among all sessions and keep alive until the Spark application terminates
    df.createGlobalTempView("people")
    spark.sql("SELECT * FROM global_temp.people").show()
//    spark.newSession().sql("SELECT * FROM global_temp.people").show()


    //  ----------- Creating DataSet -----------

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS() // org.apache.spark.sql.Dataset[Int] = [value: int]
    primitiveDS.show()
    primitiveDS.map(_ + 1).collect()


//    // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
//    // you can use custom classes that implement the Product interface
//    case class Person(name: String, age: Long)

    // option 1: create directly
    val people1DS = Seq(Person("P1", 32), Person("P2", 22)).toDS() // org.apache.spark.sql.Dataset[Person] = [name: string, age: bigint]
    people1DS.show()

    // option 2: convert DataFrame to DataSet
    val people2DS = spark.read.json(people_json_path).as[Person] // org.apache.spark.sql.Dataset[Person] = [age: bigint, name: string]
    people2DS.show()


    //  ----------- Interoperating with RDDs -----------

    // option 1: Inferring the Schema Using Reflection
    val people_txt_path = "examples/src/main/resources/people.txt"
    val peopleRDD = spark.sparkContext
      .textFile(people_txt_path)
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt)) // org.apache.spark.rdd.RDD[Person]

    val peopleDF = peopleRDD.toDF() // org.apache.spark.sql.DataFrame = [name: string, age: bigint]
    peopleDF.show()

    // works
//    val peopleDS = peopleRDD.toDS() // org.apache.spark.sql.Dataset[Person] = [name: string, age: bigint]
//    peopleDS.show()  // readable table
//    peopleDS.collect() // Array[Person] = Array(Person(Michael,29), Person(Andy,30), Person(Justin,19))

    // works
//    peopleDF.filter($"age" > 19 && $"age" < 30) // org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [name: string, age: bigint]

    peopleDF.createOrReplaceTempView("people")
    val teenagersDF = spark.sql(
      "SELECT name, age FROM people WHERE age BETWEEN 13 AND 49"
    ) // org.apache.spark.sql.DataFrame = [name: string, age: bigint]
    teenagersDF.show()
    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    // Else, will say: "error: Unable to find encoder for type stored in a Dataset.  Primitive types (Int, String, etc) and Product types (case classes) are supported by importing spark.implicits._  Support for serializing other types will be added in future releases."
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]  // mapEncoder: org.apache.spark.sql.Encoder[Map[String,Any]] = class[value[0]: binary]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    val teenagersDS = teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))) // org.apache.spark.sql.Dataset[Map[String,Any]] = [value: binary]
    teenagersDS.show()  // un-readable: just binary value
    teenagersDS.collect()  // readable: Array[Map[String,Any]] = Array(Map(name -> Michael, age -> 29), Map(name -> Andy, age -> 30), Map(name -> Justin, age -> 19))


    // option 2: programmatically specifying the schema
    runProgramSchema(spark)


    spark.stop()
  }

  private def runProgramSchema(spark: SparkSession): Unit = {
    val people_txt_path = "examples/src/main/resources/people.txt"
    val peopleRDD = spark.sparkContext.textFile(people_txt_path)

    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim)) // Array[org.apache.spark.sql.Row] = Array([Michael,29], [Andy,30], [Justin,19])

    val schemaString = "name age"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable=true)) // Array[org.apache.spark.sql.types.StructField] = Array(StructField(name,StringType,true), StructField(age,StringType,true))
    val schema = StructType(fields) // org.apache.spark.sql.types.StructType = StructType(StructField(name,StringType,true), StructField(age,StringType,true))

    val peopleDF = spark.createDataFrame(rowRDD, schema) // org.apache.spark.sql.DataFrame = [name: string, age: string]
    peopleDF.show()

  }

}
