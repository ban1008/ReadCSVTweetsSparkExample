import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSpec
import org.apache.spark.sql.SparkSession
import org.basic.example.TwitterHashtag

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("spark test example")
      .getOrCreate()
  }

}


class CountPopularHashtagsTest
  extends FunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

  it("counts hashtag occurrence and sorts in descending order") {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val testSchema = List(
      StructField("text", StringType, true),
    )

    val testData = Seq(
      Row("#crypto is here"),
      Row("#crypto rocks!!"),
      Row("#bitcoin")
    )

    val testDF = spark.createDataFrame(
      spark.sparkContext.parallelize(testData),
      StructType(testSchema)
    )

    val actualDF = TwitterHashtag.countPopularHashtags(testDF)

    val expectedSchema = List(
      StructField("value", StringType, true),
      StructField("count", LongType, false)
    )

    val expectedData = Seq(
      Row("#crypto", 2.toLong),
      Row("#bitcoin", 1.toLong)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    assertSmallDataFrameEquality(actualDF, expectedDF)

  }

}