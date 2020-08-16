package metro.pings

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
import java.time.ZoneOffset.UTC

object PingsGetter {

	val agency_ids = Seq("lametro", "lametro-rail")
	val pingsPath = os.pwd / "data" / "pings"

	def main(args: Array[String]): Unit = {
		for (agency_id <- agency_ids) {
			val (pings, dt) = get(agency_id)
			val path = pingsPath / s"agency_id=$agency_id" / s"local_date=${dt.toLocalDate}" / s"${dt.toEpochSecond(UTC)}.json"
			os.write(path, pings.mkString("\n"), createFolders=true)
  	}
	}

	def get(agency_id: String): (Seq[String], LocalDateTime) = {
		val r = requests.get(s"http://api.metro.net/agencies/$agency_id/vehicles/")
		val data = ujson.read(r.text)
		val responseDt = LocalDateTime.parse(r.headers("date")(0), RFC_1123_DATE_TIME)
		val pings = for (item <- data("items").arr) yield {
			item("event_ts") = ujson.Num(responseDt.minusSeconds(item("seconds_since_report").num.toLong).toEpochSecond(UTC))
			ujson.write(item)
		}
		(pings, responseDt)
	}
}

