package com.amadeus.elastic_scaling

import com.amadeus.elastic_scaling.cluster_interface.{DatabricksClusterInterface, NoOp}
import org.apache.spark.sql.SparkSession

import java.time._
import com.amadeus.elastic_scaling.config.ElasticConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.metrics.source.{ DoubleAccumulatorSource , LongAccumulatorSource}
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
 * @param configuredBatchDuration the configured batch duration in ms
 *
 */
class ElasticScaling(val configuredBatchDuration: Long, val spark: SparkSession, val elasticConf: ElasticConfig)
  extends StreamingQueryListener {
  
  val logger: Logger = Logger.getLogger(this.getClass)

  val cluster_interface: ClusterInterface = ClusterInterface.getInterface(spark)
  
  val thresholdHigh: Double = elasticConf.thresholdHigh
  val thresholdLow: Double = elasticConf.thresholdLow
  val maxExecutors: Int = elasticConf.executorsMax
  val minExecutors: Int = elasticConf.executorsMin
  val active: Boolean = elasticConf.active
  val alpha: Double = elasticConf.executorsSmoothingFactor
  
  var num: Double = 1.0
  var den: Double = 1.0
  
  var previousInstant: Option[java.time.Instant] = None

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
    val executors = cluster_interface.getWorkers
    val currentExecutorNumber = executors.length
  
    logger.info(s"Executors IDs (${currentExecutorNumber}): ${executors}")
  
    val neededWorkers : Int = computeExecutorNumber(meanOccupation, currentExecutorNumber)

    val scaling = cluster_interface.isClusterScaling

    if (!scaling && cluster_interface.setWorkers(neededWorkers)) {
      logger.info(s"New workers request successful.")
    } else if (scaling) {
      logger.info(s"Cluster is already scaling... ")
    }
    else {
      logger.error(s"New workers request failed.")
    }
  
    neededWorkers
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

  }
  
  /**
   * Update the occupation vector, and return the mean occupation value for the last N
   * iterations (N being occupations length). It uses an alpha factor to weight past iterations
   *
   * @param usageRatio actual occupation rate
   * @return mean occupation percentage
   */
  private def updateOccupation(usageRatio: Double): Double = {
    num = num * alpha + usageRatio
    den = den * alpha + 1
  
    val meanRatio = num / den

    logger.debug(s"New num: ${num}, new den: $den")
    logger.info(f"Instantaneous usage ratio : $usageRatio%.3f. Mean occupation ratio: ${meanRatio}%.3f")

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

    val neededWorkers = scaleExecutors(meanOccupation)
    
    showMetrics(usageRatio,
      neededWorkers,
      calculatedBatchDuration,
      triggerExecution)
  }

  
  def showMetrics(capacityRate: Double,
                  neededExecutors: Long,
                         calculatedBatchDuration: Long,
                         triggerExecution: Long
                         ): Unit = {

    /* Standard implementation: log the data */
    logger.info(f"Capacity rate: ${capacityRate}%.2f")
    logger.info(s"Calculated batch duration: ${calculatedBatchDuration}")

    logger.info(s"Number of actual workers: ${cluster_interface.getWorkers}")
    logger.info(s"Needed number of workers: ${neededExecutors}")
    logger.info(s"Requested number of workers: ${cluster_interface.getRequestedWorkers}")
    logger.info(s"Trigger execution: ${triggerExecution}")

  }
}