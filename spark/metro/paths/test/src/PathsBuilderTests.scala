package metro.paths

import utest._
import org.apache.spark.sql.SparkSession

trait TestSpark {
	lazy implicit val spark = SparkSession
		.builder()
		.master("local")
		.appName("TestPathsSparkSession")
		.config("spark.sql.shuffle.partitions", "1")
		.getOrCreate()
}

object PathsBuilderTests extends TestSuite with TestSpark {
	
	val tests = Tests {
		import spark.implicits._
		// FIXME: Figure out proper tests
	}
}
