package metro.pings

import utest.{test, Tests, TestSuite}

object PingsGetterTests extends TestSuite {
	val tests = Tests {
		val (pings, dt) = PingsGetter.get("lametro")
		test("Non-zero pings read") { pings.length > 0 }
 	}
}
	
