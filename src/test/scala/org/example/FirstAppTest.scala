package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.example.FirstApp.{averageRating, booksCountInRange, ratingMoreThan}
import org.scalatest.funsuite.AnyFunSuite
import org.test.SparkSessionTestWrapper


class FirstAppTest extends AnyFunSuite with SparkSessionTestWrapper{

  import spark.implicits._
  val path = "src/test/resources/books_test.csv"
  val data: DataFrame = spark.read.option("header", "true").csv(path)

  test("ratingMoreThan - correct") {
    val rating = 4
    val result = ratingMoreThan(data, rating)
    result.select(col("average_rating")).printSchema()
    val resultCollect = result.select(col("average_rating")).collect()

    assert(resultCollect.length == 10, "Check that result count is same 10")
    resultCollect.foreach(resultItem => {
      val ratingValue = resultItem.get(0).toString.toDouble
      assert(ratingValue >= rating.toDouble, "Check that each item in result is equal or more than 4")
    }
      )
  }

  test("ratingMoreThan - rating column does not exist") {
    val rating = 4
    val dataWithoutRating = data.select(col("bookID"), col("title"), col("isbn"))
    intercept[IllegalArgumentException] {
      ratingMoreThan(dataWithoutRating, rating)
    }
  }

  test("averageRating - correct") {
    val avgResult = averageRating(data).collect()
    val expectedAvg = 3.52

    assert(avgResult(0).getAs[Double](0) == expectedAvg, "Check that average is same as expected value")
    }

  test("averageRating - rating column does not exist") {
    val dataWithoutRating = data.select(col("bookID"), col("title"), col("isbn"))
    intercept[IllegalArgumentException] {
      averageRating(dataWithoutRating)
    }
  }

  test("booksCountInRange - correct") {
    val ranges: List[(Int, Int)] = List((0, 1))
    val result = booksCountInRange(data, ranges)

    val checkData = data
      .select(col("average_rating")
        .cast(DoubleType))
      .where("average_rating >= 0 AND average_rating <= 1")

    assert(result.columns.length.equals(2), "Check that only 2 columns exist")
    assert(result.columns.contains("range"), "Check that range column exist in dataframe")
    assert(result.columns.contains("count"), "Check that count column exist in dataframe")
    assert(result.select("count").collect()(0).getAs[Long](0) == checkData.count(), "Check that book count with rating (0,1) is same as check data")
    assert(result.select("range").collect()(0).getAs[String](0) == "0 - 1", "Check that range is same as ranges list")
  }

  test("booksCountInRange - same range") {
    val ranges: List[(Int, Int)] = List((0, 0))
    val result = booksCountInRange(data, ranges)

    val checkData = data
      .select(col("average_rating")
        .cast(DoubleType))
      .where("average_rating >= 0 AND average_rating <= 0")

    assert(result.select("count").collect()(0).getAs[Long](0) == checkData.count(), "Check that book count with rating (0,1) is same as check data")
  }

  test("booksCountInRange - not correct range") {
    val ranges: List[(Int, Int)] = List((1, 0))
    intercept[IllegalArgumentException] {
      booksCountInRange(data, ranges)
    }

  }
}
