package metro.paths.geojson

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection
import java.time.LocalDateTime
import java.time.ZoneOffset.UTC
import ujson.{Arr, Num, Str, Obj, write}

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

trait Spark {
	lazy implicit val spark = SparkSession
		.builder()
		.master("local")
		.appName("PathsGeoJsonSparkSession")
		.config("spark.sql.shuffle.partitions", "1")
		.getOrCreate()
}

object GeoJson extends Spark {
	val pathsBasePath = os.pwd / "data" / "paths"
	val pathsGeojsonBasePath = os.pwd / "data" / "geojson"

	def main(args: Array[String]): Unit = {
		val (agency_id, local_date, id, route_id) = ("lametro", "2020-08-17", None, None)  // TODO: Rm tmp test args, use Scallop
		val pathsDs = readPaths(agency_id, local_date, id, route_id)
		val fc = makeFeatureCollection(pathsDs.collect)
    val fileName = Seq(agency_id, local_date, id.getOrElse("ALL"), route_id.getOrElse("ALL")).mkString("_") + ".geojson"	
		os.write.over(pathsGeojsonBasePath / fileName, ujson.write(fc), createFolders=true)
	}

	def readPaths(
		agency_id: String, local_date: String, id: Option[String] = None, route_id: Option[String] = None
	)(implicit spark: SparkSession): Dataset[Path] = {
		import spark.implicits._
		spark
			.read
			.schema(ScalaReflection.schemaFor[Path].dataType.asInstanceOf[StructType])
			.json(pathsBasePath.toString)
			.as[Path]
			.filter(p => 
				p.agency_id == agency_id && 
				p.local_date == local_date && 
				id.getOrElse(p.id) == p.id && 
				route_id.getOrElse(p.route_id) == p.route_id
			)
	}

	def makeFeatureCollection(paths: Seq[Path]): Obj = {
		Obj(
			"type" -> "FeatureCollection",
			"features" -> Arr(paths.map(makeFeature):_*)
		)
	}

	def makeFeature(path: Path): Obj = Obj(
		"type" -> "Feature",
		"geometry" -> Obj(
			"type" -> "LineString",
			"coordinates" -> Arr(path.coordinates.map(c => Arr(Num(c.lng), Num(c.lat), Num(c.alt), Num(c.ts.toInt))):_*)
		),
		"properties" -> Obj(
			"id" -> Str(path.id),
			"agency_id" -> Str(path.agency_id),
			"local_date" -> Str(path.local_date),
			"route_id" -> Str(path.route_id),
			"run_id" -> Str(path.run_id.getOrElse("NONE"))
		)
	)
}
