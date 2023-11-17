package com.amadeus.elastic_scaling

import com.amadeus.elastic_scaling.cluster_interface.{DatabricksClusterInterface, NoOp}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/** Defines the ClusterInterface trait for obtaining information about a Spark cluster.
 */

/**
 *  Interacts with a Spark cluster and retrieves cluster-related information.
 */
trait ClusterInterface {
  /**
   * Get a sequence of WorkerInfo objects representing the worker nodes in the Spark cluster.
   *
   * This method returns a collection of [[WorkerInfo]] objects, describing the nodes available in the Spark cluster.
   * Note that this MUST be implemented in order to consider tha actual number of workers
   * present in the cluster, and not the requested number of nodes.
   *
   * This MUST only updates when a node is actually added or removed from the cluster.
   *
   * @return A sequence of WorkerInfo objects representing the worker nodes actually active in the cluster.
   *
   */
  def getWorkers: Seq[WorkerInfo]
  def getRequestedWorkers: Int
  def setWorkers(numWorkers: Int): Boolean

  /**
   * Determine if the cluster is already taking into account an request to scale up or down
   *
   * @return true if there is already a scaling request, false otherwise
   */
  def isClusterScaling: Boolean = false
}

object ClusterInterface {

  /**
   * Instanciate a concrete implementation for this interface, based on the configuration.
   *
   * //TODO: implement it via reflection, in order to be able to have other implementation
   *
   * */
  def getInterface(spark: SparkSession): ClusterInterface = {
    val l = Logger.getLogger("ElasticScaling")
    spark.conf.getOption("com.amadeus.elastic_scaling.cluster_interface") match {
      //TODO: correctly use reflection in order to support different cluster interfaces
      case Some(fqn) if (fqn == "com.amadeus.elastic_scaling.cluster_interface.DatabricksClusterInterface") => {
        l.info("Cluster interface configured: $fqn")
        DatabricksClusterInterface()(spark)
      }
      case _ =>
        l.info("Cluster interface configured as No Operation")
        NoOp()
    }
  }

}