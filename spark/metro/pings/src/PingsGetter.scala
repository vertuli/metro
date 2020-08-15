package metro.pings

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
import java.time.ZoneOffset.UTC

object PingsGetter {
	def get(agency_id: String): (Seq[String], LocalDateTime) = {
		val r = requests.get(s"http://api.metro.net/agencies/$agency_id/vehicles/")
		val data = ujson.read(r.text)
		val pings = for (item <- data("items").arr) yield ujson.write(item)
		val dt = LocalDateTime.parse(r.headers("date")(0), RFC_1123_DATE_TIME)
		(pings, dt)
	}
}

