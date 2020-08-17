package metro.paths

import org.apache.spark.sql.{Dataset, SparkSession}

case class Ping(
	id: String, 
	agency_id: String, 
	local_date: String, 
	route_id: String, 
	run_id: Option[String], 
	longitude: Double, 
	latitude: Double, 
	event_ts: Double
)

trait Spark {
	lazy implicit val spark = SparkSession
		.builder()
		.master("local")
		.appName("PathsSparkSession")
		.config("spark.sql.shuffle.partitions", "1")
		.getOrCreate()
}

object PathsBuilder extends Spark {
	val pingsBasePath = os.pwd / "data" / "pings"

	def main(args: Array[String]): Unit = {
		val pingsDs = readPings
		println(pingsDs.count)
	}

	def readPings(implicit spark: SparkSession): Dataset[Ping] = {
		import spark.implicits._
		spark.read.json(pingsBasePath.toString).as[Ping]
	}
}

