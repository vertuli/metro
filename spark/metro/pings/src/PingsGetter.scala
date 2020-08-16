package metro.pings

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
import java.time.ZoneOffset.UTC

object PingsGetter {
	val agency_ids = Seq("lametro", "lametro-rail")
	val pingsBasePath = os.pwd / "data" / "pings"

	def main(args: Array[String]): Unit = {
		println("Starting...")
		while(true) {
			for (agency_id <- agency_ids) {
				get(agency_id)  // TODO: handle errors from metro API
			}
			Thread.sleep(60000)
		}
	}

	def get(agency_id: String): Unit = {
		val (pings, dt) = request(agency_id)
		val subPath = os.SubPath(s"agency_id=$agency_id/local_date=${dt.toLocalDate}/${dt.toEpochSecond(UTC)}.json")
		os.write(pingsBasePath / subPath, pings.mkString("\n"), createFolders=true)
		println(s"$dt - ${pings.length} pings written to $subPath")  // TODO: logging
	}

	def request(agency_id: String): (Seq[String], LocalDateTime) = {
		val r = requests.get(s"http://api.metro.net/agencies/$agency_id/vehicles/")
		val data = ujson.read(r.text)
		val responseDt = LocalDateTime.parse(r.headers("date")(0), RFC_1123_DATE_TIME)
		val pings = for (item <- data("items").arr) yield {
			val eventDt = responseDt.minusSeconds(item("seconds_since_report").num.toLong)
			item("event_ts") = ujson.Num(eventDt.toEpochSecond(UTC))
			ujson.write(item)
		}
		(pings, responseDt)
	}
}

