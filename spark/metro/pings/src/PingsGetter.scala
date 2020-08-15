package metro.pings

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
import java.time.ZoneOffset.UTC

object PingsGetter {

	val agency_ids = Seq("lametro", "lametro-rail")

	def main(args: Array[String]): Unit = {
		for (agency_id <- agency_ids) {
			val (pings, dt) = get(agency_id)
			val path = os.pwd / "data" / "pings" / s"agency_id=$agency_id" / s"response_ts=${dt.toEpochSecond(UTC)}" / "pings.json"
			os.write(path, pings.mkString("\n"), createFolders=true)
  	}
	}

	def get(agency_id: String): (Seq[String], LocalDateTime) = {
		val r = requests.get(s"http://api.metro.net/agencies/$agency_id/vehicles/")
		val data = ujson.read(r.text)
		val pings = for (item <- data("items").arr) yield ujson.write(item)
		val dt = LocalDateTime.parse(r.headers("date")(0), RFC_1123_DATE_TIME)
		(pings, dt)
	}
}

