package metro.paths.geojson

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection

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

case class Key(agency_id: String, local_date: String)

case class Feature(`type`: String = "Feature", geometry: Geometry, properties: Properties)

case class Geometry(`type`: String = "LineString", coordinates: Seq[(Double, Double, Double, Int)])

case class Properties(id: String, agency_id: String, local_date: String, route_id: String, run_id: String)

case class FeatureCollection(`type`: String = "FeatureCollection", features: Seq[Feature])

object GeoJson extends Spark {
	val pathsBasePath = os.pwd / "data" / "paths"
	val geoJsonBasePath = os.pwd / "data" / "geojson"

	def main(args: Array[String]): Unit = {
		import spark.implicits._
		val pathsDs = spark
			.read
			.schema(ScalaReflection.schemaFor[Path].dataType.asInstanceOf[StructType])
			.json(pathsBasePath.toString)
			.as[Path]
			.groupByKey(p => Key(p.agency_id, p.local_date))
			.mapGroups(makeFeatureCollection)
			.foreach(writeGeoJson) // FIXME: Have foreach() call this method properly
	}

	def makeFeatureCollection(key: Key, paths: Iterator[Path]): FeatureCollection = {
		FeatureCollection(features=paths.map(makeFeature).toSeq)
	}

	def makeFeature(path: Path): Feature = Feature(
		geometry=Geometry(coordinates=path.coordinates.map(c => (c.lng, c.lat, c.alt, c.ts.toInt))), 
		properties=Properties(
			id=path.id,
			agency_id=path.agency_id,
			local_date=path.local_date,
			route_id=path.route_id,
			run_id=path.run_id.getOrElse("NONE")
		)
	)

	def writeGeoJson(fc: FeatureCollection): Unit = {
		val agency_id = fc.features.head.properties.agency_id
		val local_date = fc.features.head.properties.local_date
		// FIXME: Serialize the FeatureCollection case class to JSON
		os.write(os.pwd / s"${agency_id}_${local_date}_paths.geojson", ujson.write(fc, indent=4), createFolders=true)
	}
}
