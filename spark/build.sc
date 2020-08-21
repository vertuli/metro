import mill._
import mill.scalalib._
import coursier.maven.MavenRepository

object metro extends ScalaModule {
	def scalaVersion = "2.12.12"

	object pings extends ScalaModule {
		def scalaVersion = "2.12.12" 
		def ivyDeps = Agg(
			ivy"com.lihaoyi::requests:0.6.5",
			ivy"com.lihaoyi::upickle:0.9.5",
			ivy"com.lihaoyi::os-lib:0.7.1"
		)
		object test extends Tests {
			def ivyDeps = Agg(
				ivy"com.lihaoyi::utest:0.7.2"
			)
			def testFrameworks = Seq("utest.runner.Framework")
		}
	}
	
	object paths extends ScalaModule {
		def scalaVersion = "2.12.12"
		def repositories = super.repositories ++ Seq(
			MavenRepository("http://dl.bintray.com/spark-packages/maven")
		)
		def ivyDeps = Agg(
			ivy"org.apache.spark::spark-sql:3.0.0",
			ivy"com.lihaoyi::os-lib:0.7.1"
		)
		object test extends Tests {
			def ivyDeps = Agg(
				ivy"com.lihaoyi::utest:0.7.2"
			)
			def testFrameworks = Seq("utest.runner.Framework")
		}
		object geojson extends ScalaModule {
			def scalaVersion = "2.12.12"
			def ivyDeps = Agg(
				ivy"org.apache.spark::spark-sql:3.0.0",
				ivy"com.lihaoyi::os-lib:0.7.1",
				ivy"com.lihaoyi::upickle:0.9.5"
			)
		}
	}
}

