package org.apache.spark

import org.apache.spark.sql.SparkSession

object ExecutorScaling {
  private def getListOfExecutors(spark: SparkSession) : Seq[String] = {
    spark
      .sparkContext
      .getExecutorIds()
      //to have them sorted
      .map(_.toInt)
      .sorted
      .map(_.toString)
  }
}