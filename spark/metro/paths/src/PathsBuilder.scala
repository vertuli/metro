package metro.paths

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.streaming.StreamingQuery

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

case class Coordinate(
	lng: Double,
	lat: Double,
	alt: Double,
	ts: Double
)

case class Path(
	id: String,
	agency_id: String,
	route_id: String,
	run_id: Option[String],
	coordinates: Seq[Coordinate]
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
		val query = startPathsStream
		query.awaitTermination()
	}

	def startPathsStream(implicit spark: SparkSession): StreamingQuery = {
		import spark.implicits._
		spark
			.readStream
			.schema(ScalaReflection.schemaFor[Ping].dataType.asInstanceOf[StructType])
			.option("maxFilesPerTrigger", 2)
			.json(pingsBasePath.toString)
			.as[Ping]
			.groupByKey(p => (p.id, p.agency_id, p.route_id, p.run_id))
			.mapGroups{ case ((id, agency_id, route_id, run_id), pings) => {
				val coords = pings.toSeq.sortBy(_.event_ts).map{ p => Coordinate(p.longitude, p.latitude, 0.0, p.event_ts) }
				Path(id, agency_id, route_id, run_id, coords)
				}
			}
			.writeStream
			.queryName("paths")
			.format("memory")
			.outputMode("update")
			.start()
	}
}

