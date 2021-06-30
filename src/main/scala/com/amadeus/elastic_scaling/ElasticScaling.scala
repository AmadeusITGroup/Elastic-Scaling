package com.amadeus.elastic_scaling

import org.apache.spark.sql.SparkSession
import java.time._
import com.amadeus.elastic_scaling.config.ElasticConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import scala.collection.JavaConverters.mapAsScalaMapConverter


object ElasticScaling {
  def apply(configuredBatchDuration: Long, spark: SparkSession) : ElasticScaling = {
    val elasticConf: ElasticConfig = ElasticConfig(spark)
    new ElasticScaling(configuredBatchDuration, spark, elasticConf)
  }
}

/**
 * This listener is used to scale the resource utilisation of a streaming job.
 *
 * To use it, a configuration node need to be added to the application configuration.
 *
 * @example An example of node configuration:
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
 * @param configuredBatchDuration the configured batch duration in ms
 *
 */
class ElasticScaling(val configuredBatchDuration: Long, val spark: SparkSession, val elasticConf: ElasticConfig)
  extends StreamingQueryListener {
  
  val logger: Logger = Logger.getLogger(this.getClass)
  
  val thresholdHigh: Double = elasticConf.thresholdHigh
  val thresholdLow: Double = elasticConf.thresholdLow
  val maxExecutors: Int = elasticConf.executorsMax
  val minExecutors: Int = elasticConf.executorsMin
  val active: Boolean = elasticConf.active
  val alpha: Double = elasticConf.executorsSmoothingFactor
  
  var num: Double = 0.0
  var den: Double = 0.0
  
  var previousInstant: Option[java.time.Instant] = None
  var occupation = Seq(0.87, 0.87, 0.87, 0.87, 0.87) //Initialization. Calculated on 5 points, between 1.0 and 0.75
  
  initListener()
  
  
  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
    logger.info("Query started: " + queryStarted.id)
  }
  
  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
    logger.info("Query terminated: " + queryTerminated.id)
  }
  
  /**
   * Compute the needed number of executors, given the mean occupation and the current number of executors
   *
   * @param meanOccupation the calculated mean occupation
   * @param currentExecutorNumber the current number of executors
   * @return the needed number of executor
   */
  def computeExecutorNumber(meanOccupation: Double, currentExecutorNumber: Int) : Int = {
    if (meanOccupation > thresholdHigh || meanOccupation < thresholdLow) {
      //Bound to [minExecutors, maxExecutors]
      val neededTotalExec: Int = math.max(math.min(math.ceil(currentExecutorNumber * meanOccupation), maxExecutors), minExecutors).toInt
      logger.info(s"New total number of executors: $neededTotalExec")
      neededTotalExec
    } else {
      logger.info(s"Nothing to change as mean occupation ( ${meanOccupation} ) " +
        s"is in configured range [$thresholdLow, $thresholdHigh]")
      currentExecutorNumber
    }
  }
  
  /**
   * implement the logic to calculate the new number of executors, based on the mean occupation
   *
   * @param meanOccupation calculated mean occupation
   * @return the new needed number of executors
   *
   */
  private def scaleExecutors(meanOccupation: Double): Long = {
    //Remove one executor, unless we have only minExecutors
    //Executors are sorted in order to remove the oldest one
    val executors = org.apache.spark.ExecutorScaling
      .getListOfExecutors(spark)
    val currentExecutorNumber = executors.length
  
    logger.info(s"Executors IDs (${currentExecutorNumber}): ${executors}")
  
    val newExecutorNumber : Int = computeExecutorNumber(meanOccupation, currentExecutorNumber)
    val newRequest: Int = newExecutorNumber - currentExecutorNumber
  
    if (newRequest > 0) {
      requestExecutors(newRequest)
    }
    else if (newRequest < 0) {
      //Kill old executors
      releaseExecutors(math.abs(newRequest))
    }
    else {
      logger.info(s"No need to modify executor number")
    }
  
    newExecutorNumber
    
  }
  
  /**
   * Actually call the spark API to release the selected number of executors.
   * Check if elastic scaling is `active` in the configuration
   *
   * @param toBeKilledNumber number of executors to be released
   */
  private def releaseExecutors(toBeKilledNumber: Int): Unit = {
    val executors = org.apache.spark.ExecutorScaling
      .getListOfExecutors(spark)
  
    val toBeKilledExec = executors.take(toBeKilledNumber)
  
    if (active) {
      val res = spark.sparkContext.killExecutors(toBeKilledExec)
      logger.info(s"Executors to be killed (${toBeKilledNumber}): $toBeKilledExec. Result: ${res}")
    }
    else {
      logger.warn(s"Elastic scaling has been disabled in configuration. " +
        s"Release of ${toBeKilledExec} executors not done")
    }
  }
  
  /**
   * Actually call the spark API to request new executors
   * Check if elastic scaling is `active` in the configuration
   *
   * @param newRequest number of new executors to be requested
   *
   */
  private def requestExecutors(newRequest: Int): Unit = {
    if (active) {
      val newReq = spark.sparkContext.requestExecutors(newRequest)
      logger.info(s"New Executor has been requested and granted: ${newReq}")
    }
    else {
      logger.warn(s"Elastic scaling has been disabled in configuration. " +
        s"Acquisition of ${newRequest} new executors not done")
    }
  }
  
  /**
   * Implement logic in order to choose between the configured and calculated batch duration
   *
   * @return
   */
  private def selectDuration(configured: Double, calculated: Double): Double = {
    configured
  }
  
  /**
   * Initialize the Listener
   */
  def initListener(): Unit = {
    if (!active) {
      logger.setLevel(Level.OFF)
    }
  
    if (minExecutors < 1) {
      throw new Exception("Scaling.Executors.Min must be >= 1")
    }
  
    //This is done to have control on the executors number, otherwise the first N
    //requests to new executors are not fulfilled
    //At the moment we ignore the task locality preferences, as there is no stage launched yet.
    val executorsN = org.apache.spark.ExecutorScaling.getListOfExecutors(spark).length
    val res = spark.sparkContext.requestTotalExecutors(executorsN, 0, Map[String, Int]())
  
    logger.info(s"Reallocating initial executors (${executorsN}): $res")
  
    //Init occupation mean
    Range(0, 5).foreach(_ => updateOccupation((thresholdHigh + thresholdLow) / 2, false))
  
  }
  
  /**
   * Update the occupation vector, and return the mean occupation value for the last N
   * iterations (N being occupations length). It uses an alpha factor to weight past iterations
   *
   * @param usageRatio actual occupation rate
   * @return mean occupation percentage
   */
  private def updateOccupation(usageRatio: Double, verbose: Boolean = true): Double = {
    num = num * alpha + usageRatio
    den = den * alpha + 1
  
    val meanRatio = num / den
  
    if (verbose) {
      logger.debug(s"New num: ${num}, new den: $den")
      logger.info(s"Instantaneous usage ratio : $usageRatio")
      logger.info(s"Mean occupation ratio: ${meanRatio}")
    }
  
    meanRatio
  }
  
  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
    if (!active) {
      //Elastic scaling is disabled in conf, doing nothing
      logger.info("Elastic scaling has been deactivated. Doing nothing")
      return
    }
    
    if (!queryProgress.progress.durationMs.containsKey("addBatch")) {
      //In this case, there is no data to be processed, nothing has been done
      //This may happen if this MicroBatch duration is << of source data rate
      logger.info("Query without progress. Skipping resource calculation")
      return
    }
    
    val triggerExecution: Long = queryProgress.progress.durationMs.asScala("triggerExecution")
    val currentInstant = Instant.parse(queryProgress.progress.timestamp)
    
    if (previousInstant.isEmpty) {
      previousInstant = Option(currentInstant)
      logger.info("This is the first iteration batch duration calculation will be 0")
    }
    
    val calculatedBatchDuration = Duration.between(previousInstant.get, currentInstant).toMillis
    logger.info(s"Calculated microBatch interval is ${calculatedBatchDuration}ms - (${calculatedBatchDuration / 1000d}s)")
    logger.info(s"Configured microBatch interval is ${configuredBatchDuration}ms - (${configuredBatchDuration / 1000d}s)")
    previousInstant = Option(currentInstant)
    
    val usageRatio = triggerExecution / selectDuration(configuredBatchDuration, calculatedBatchDuration)
    
    val meanOccupation = updateOccupation(usageRatio)
    
    val actualExecutors = org.apache.spark.ExecutorScaling.getListOfExecutors(spark).length
    val neededExecutors = scaleExecutors(meanOccupation)
    
    showMetrics(usageRatio,
      actualExecutors,
      neededExecutors,
      calculatedBatchDuration,
      triggerExecution)
  }

  
  def showMetrics(capacityRate: Double,
                  numOfExecutors: Long,
                  neededExecutors: Long,
                         calculatedBatchDuration: Long,
                         triggerExecution: Long
                         ): Unit = {
  
    /* Standard implementation: log the data */
    logger.info(s"Capacity rate: ${capacityRate}")
    logger.info(s"Number of executors: ${numOfExecutors}")
    logger.info(s"Needed executors: ${neededExecutors}")
    logger.info(s"Calculated batch duration: ${calculatedBatchDuration}")
    logger.info(s"Trigger execution: ${triggerExecution}")
  
  }
}

