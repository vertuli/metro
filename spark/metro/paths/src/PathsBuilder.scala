package metro.paths

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQuery}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.ZoneOffset.UTC

case class Ping(
	id: String, 
	agency_id: String, 
	local_date: String, 
	route_id: String, 
	run_id: Option[String], 
	longitude: Double, 
	latitude: Double, 
	event_ts: Timestamp
)

case class Coordinate(
	lng: Double,
	lat: Double,
	alt: Double,
	ts: Long
)

case class Path(
	id: String,
	agency_id: String,
	route_id: String,
	run_id: Option[String],
	var local_date: String,
	var latest_ts: Long,
	var coordinates: Seq[Coordinate]
)

case class Key(id: String, agency_id: String, route_id: String, run_id: Option[String])

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
	val pathsBasePath = os.pwd / "data" / "paths"
	val pathsCheckpointPath = os.pwd / "data" / "checkpoints" / "paths"

	def main(args: Array[String]): Unit = {
		import spark.implicits._
		val pathsQuery = getPingStream
			.groupByKey(p => Key(p.id, p.agency_id, p.route_id, p.run_id))
			.flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout)(updatePath)
			.writeStream
			.partitionBy("agency_id", "local_date")
			.format("json")
			.outputMode("append")
			.option("path", pathsBasePath.toString)
			.option("checkpointLocation", pathsCheckpointPath.toString)
			.start()

		pathsQuery.awaitTermination()
	}

	def getPingStream(implicit spark: SparkSession): Dataset[Ping] = {
		import spark.implicits._
		spark
			.readStream
			.schema(ScalaReflection.schemaFor[Ping].dataType.asInstanceOf[StructType])
			.option("maxFilesPerTrigger", 10)
			.json(pingsBasePath.toString)
			.withWatermark("event_ts", "5 seconds")
			.dropDuplicates("id", "agency_id", "route_id", "run_id", "event_ts")
			.as[Ping]
			.filter(p => p.longitude != 0 && p.latitude != 0)  // filters weird (0, 0) pings from la metro
	}

	def updatePath(key: Key, pings: Iterator[Ping], state: GroupState[Path]): Iterator[Path] = {
		pings.toSeq.sortBy(_.event_ts.getTime).toIterator.flatMap { ping =>
			if (!state.exists) {
				state.update(newPathFromPing(ping))
				Iterator()
			} else {
				val path = state.get
				if (ping.event_ts.toLocalDateTime.toEpochSecond(UTC) > path.latest_ts + 600) {
					state.remove()
					state.update(newPathFromPing(ping))
					Iterator(path)
				} else {
					state.update(updatedPathWithPing(path, ping))
					Iterator()
				}
			}
		}
	}

	def newPathFromPing(ping: Ping): Path = {
		val ts = ping.event_ts.toLocalDateTime.toEpochSecond(UTC)
		Path(
			id=ping.id,
			agency_id=ping.agency_id,
			route_id=ping.route_id,
			run_id=ping.run_id,
			local_date=ping.local_date,
			latest_ts=ts,
			coordinates=Seq(Coordinate(ping.longitude, ping.latitude, 0.0, ts))
		)
	}

	def updatedPathWithPing(path: Path, ping: Ping): Path = {
		val newCoord = Coordinate(ping.longitude, ping.latitude, 0.0, ping.event_ts.toLocalDateTime.toEpochSecond(UTC))
		if (newCoord.ts > path.latest_ts) {
			path.coordinates = path.coordinates :+ newCoord
			path.latest_ts = newCoord.ts
		} else if (newCoord.ts < path.coordinates.head.ts) {
			path.coordinates = newCoord +: path.coordinates
			path.local_date = ping.event_ts.toLocalDateTime.toLocalDate.toString
		} else {
			// TODO: more efficient sort for inserting element to sorted list.
			path.coordinates = (path.coordinates :+ newCoord).sortBy(_.ts)
		}
		path
	}
}

