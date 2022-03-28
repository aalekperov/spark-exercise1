package org.example

import org.apache.commons.lang3.Validate
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, lit, round}
import org.apache.spark.sql.types.DoubleType

object FirstApp {

  val spark: SparkSession = SparkSession
    .builder
    .appName("First Application")
    .getOrCreate()


  def main(args: Array[String]) : Unit = {
    val data = readCsvFile(args(0))
    data.printSchema()

    println("Rows count: " + rowCount(data))

    val rating = 4.5
    println(s"Book count with rating more than $rating: " + ratingMoreThan(data, rating).count())

    println("Average rating")
    averageRating(data).show()

    println("Range of ratings:")
    val ranges: List[(Int, Int)] = List((0, 1), (1, 2), (3, 4), (4, 5))
    val result = booksCountInRange(data, ranges)
    result.show()
  }

  def readCsvFile(filePath: String): DataFrame = {
    println("Reading the file: " + filePath)
    spark.read.option("header", "true").csv(filePath)
  }

  def rowCount(data: DataFrame): Long = {
    data.count()
  }

  def ratingMoreThan(data: DataFrame, rating: Double): Dataset[Row] = {
    Validate.isTrue(data.columns.contains("average_rating"))
    data
      .filter(row => row.getAs[String]("average_rating").toDouble > rating)
  }

  def averageRating(data: DataFrame): DataFrame = {
    Validate.isTrue(data.columns.contains("average_rating"))
    data
      .select(col("average_rating")
        .cast(DoubleType)
        .as("average_rating"))
      .agg(round(avg("average_rating"), 2)
        .as("average"))
  }

  def booksCountInRange(data: DataFrame, ranges: List[(Int, Int)]): DataFrame = {
    Validate.isTrue(data.columns.contains("average_rating"))
    ranges.foreach(range => Validate.isTrue(range._1 <= range._2))
    def df(lowBound: Int, upperBound: Int) = {
      data
      .select(col("average_rating").cast(DoubleType))
      .where(col("average_rating")
        .between(lowBound, upperBound))
      .agg(count("*").as("count"))
      .withColumn("range", lit(s"${lowBound.toString} - ${upperBound.toString}"))
      .select(col("range"), col("count"))
    }

    ranges.map(range => df(range._1, range._2)).reduce(_ union _)

  }

}
