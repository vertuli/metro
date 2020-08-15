package metro

import utest.{test, Tests, TestSuite}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ExampleTests extends TestSuite {
	
	lazy val spark = SparkSession
		.builder()
		.master("local")
		.appName("local_test_spark_session")
		.config("spark.sql.shuffle.partitions", "1")
		.getOrCreate()

	val tests = Tests {
		import spark.implicits._
		val sourceData = Seq(Row("jose"), Row("li"), Row("luisa"))
		val sourceSchema = StructType(List(StructField("name", StringType, true)))
		val sourceDf = spark.createDataFrame(spark.sparkContext.parallelize(sourceData), sourceSchema)
				
		val expectedData = Seq(Row("jose", "hello world"), Row("li", "hello world"), Row("luisa", "hello world"))
		val expectedSchema = StructType(List(StructField("name", StringType, true), StructField("greeting", StringType, true)))
		val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(expectedData), expectedSchema)
			
		val actualDf = sourceDf.transform(Example.withGreeting())

		val tests = Tests {
			test("Same Schema"){ expectedDf.schema.equals(actualDf.schema) }
			test("Same Elements") { expectedDf.collect.sameElements(actualDf.collect) }
		}
	}
}
