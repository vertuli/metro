package metro

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

object Example {
  def withGreeting()(df: DataFrame): DataFrame = {
		df.withColumn("greeting", lit("hello world"))
	}
}

