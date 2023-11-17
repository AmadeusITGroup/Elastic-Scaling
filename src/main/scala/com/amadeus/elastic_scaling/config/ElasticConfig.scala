package com.amadeus.elastic_scaling.config

import org.apache.log4j.{Logger}
import org.apache.spark.sql.SparkSession

/**
 * Configuration for elastic scaling
 *
 *          Scaling {
 *            Active = false //Active the elastic scheduling: true means it will request or release executors.
 *            Threshold {
 *              High = 1.0
 *              Low  = 0.75
 *            }
 *            Executors {
 *              Max = 20
 *              Min = 1
 *              SmoothingFactor = 0.6 //How much past iteration are considered for mean utilisation.
 *                                    //Must be on [0,1] range.
 *                                    // 0 mean only current iteration is taken into account
 *                                    // 1 All iterations are considered for mean calculation
 *            }
 *          }
 *
 */
case class ElasticConfig(
                          active: Boolean,
                          thresholdHigh: Double,
                          thresholdLow : Double,
                          executorsMin : Int,
                          executorsMax: Int,
                          executorsSmoothingFactor : Double
                        )

object ElasticConfig {
  
  val logger: Logger = Logger.getLogger(this.getClass)
  
  def apply(spark: SparkSession): ElasticConfig = {

    val active: Boolean = spark.conf.getOption("com.amadeus.elastic_scaling.active").getOrElse("false").toBoolean
    val thresholdHigh: Double = spark.conf
      .getOption("com.amadeus.elastic_scaling.threshold.high").getOrElse("1.0").toDouble
    val thresholdLow: Double = spark.conf
      .getOption("com.amadeus.elastic_scaling.threshold.low").getOrElse("0.75").toDouble
    val executorsHigh: Int = spark.conf.getOption("com.amadeus.elastic_scaling.executors.max").getOrElse("10").toInt
    val executorsMin: Int = spark.conf.getOption("com.amadeus.elastic_scaling.executors.min").getOrElse("1").toInt
    val executorsSmoothingFactor: Double = spark.conf.
      getOption("com.amadeus.elastic_scaling.executors.smoothing_factor").getOrElse("0.6").toDouble
  
    
    logger.info(s"Elastic Config is Active: ${active}")
    
    new ElasticConfig(active,
      thresholdHigh,
      thresholdLow,
      executorsMin,
      executorsHigh,
      executorsSmoothingFactor)

  }
}
