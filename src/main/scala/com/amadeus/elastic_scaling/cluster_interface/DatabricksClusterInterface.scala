package com.amadeus.elastic_scaling.cluster_interface

import com.amadeus.elastic_scaling.{ClusterInterface, WorkerInfo}
import com.databricks.sdk.WorkspaceClient
import com.databricks.sdk.core.DatabricksConfig
import com.databricks.sdk.service.compute.{ClusterDetails, ResizeCluster}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

import scala.util.{Failure, Success, Try}

/**
 * This Cluster Interface implemenation allows to scale your Databricks Cluster
 * working with Structured Streamings queries.
 *
 * This implementation use the [DataBricks Java SDK](https://github.com/databricks/databricks-sdk-java)
 * for interfacing with the DataBricks platform. Some configurations keys need to be setup, in order to authentify the user
 * performing the call to the API; usually this is achieved via PAT or Service Principal authentication.
 *
 * The logic to determine if the cluster is already upscaling or not (the `isClusterScaling` method) is implemented
 * via checks on the actual active workers in the cluster, and the target worker number.
 *
 * @param sparkSession The spark session for interacting with the cluster.
 *
 */
case class DatabricksClusterInterface(implicit sparkSession: SparkSession)
  extends ClusterInterface {

  //CONFIGURATION KEYS USED TO OBTAIN NEEDED INFO
  val CONF_KEY_CLUSTERID: String = "spark.databricks.clusterUsageTags.clusterId"
  /**
   * Can be PAT or SP
   *
   * If PAT, define "spark.com.amadeus.elastic_scaling.personal_access_token"
   * If SP, define "spark.com.amadeus.elastic_scaling.ApplicationID", ApplicationPassword and TenantID.
   *
   * If not defined, or other values are used, use the default configuration, as explained in the
   * Databricks JAVA SDK documentation
   *
   */
  val CONF_KEY_AUTH_MODE: String = "com.amadeus.elastic_scaling.auth_mode"
  val CONF_KEY_PAT: String = "com.amadeus.elastic_scaling.pat.personal_access_token"

  val CONF_KEY_APPID: String = "com.amadeus.elastic_scaling.sp.ApplicationID"
  val CONF_KEY_APPPWD: String = "com.amadeus.elastic_scaling.sp.ApplicationPassword"
  val CONF_KEY_TENANTID: String = "com.amadeus.elastic_scaling.sp.TenantID"

  /**
   * Host of the databricks control plane:
   * https://adb-xxx.xx.azuredatabricks.net
   *
   * */
  val CONF_KEY_HOST: String = "com.amadeus.elastic_scaling.host"

  private val logger: Logger = Logger.getLogger(this.getClass)

  private lazy val w: WorkspaceClient = new WorkspaceClient(buildDatabricksConfiguration)

  private def buildDatabricksConfiguration: DatabricksConfig = {

    val authMode = sparkSession.conf.get(CONF_KEY_AUTH_MODE, "NOT-DEFINED")
    logger.info(s"Configuration mode for elastic scaling API: $authMode")

    /* Authentication mode Personal access token */
    val cfg = if (authMode == "PAT") {
      new DatabricksConfig()
        .setToken(sparkSession.conf.get(CONF_KEY_PAT, ""))
        .setHost(getHostName)
    } /* Authentication mode Service Principal */
    else if (authMode == "SP") {
      new DatabricksConfig()
        .setAzureClientId(sparkSession.conf.get(CONF_KEY_APPID)) //{{secrets/ds-keyvault-secrets/ApplicationID}}
        .setAzureClientSecret(sparkSession.conf.get(CONF_KEY_APPPWD)) //{{secrets/ds-keyvault-secrets/ApplicationPassword}}
        .setAzureTenantId(sparkSession.conf.get(CONF_KEY_TENANTID)) // {{secrets/ds-keyvault-secrets/TenantID}}
        .setHost(getHostName)
    } else {
      logger.warn("No Authorisation Mode selected, using default Databricks Config")
      new DatabricksConfig()
    }

    cfg
  }

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
  override def getWorkers: Seq[WorkerInfo] = {
    Option(w.clusters().get(getClusterId).getExecutors) match {
      case Some(q) => q.asScala.map(x => WorkerInfo(x.getNodeId, x.getPrivateIp)).toSeq
      case None => Nil
    }
  }

  override def getRequestedWorkers: Int = {
    w.clusters.get(getClusterId).getNumWorkers.intValue()
  }

  override def setWorkers(numWorkers: Int): Boolean = {
    //Build Request
    val resizeClusterRequest = new ResizeCluster()
      .setNumWorkers(numWorkers)
      .setClusterId(getClusterId)

    val answer = w.clusters.resize(resizeClusterRequest)

    /*
    Try(answer.get(java.time.Duration.ofSeconds(10))) match {
      case Success(cd) => cd.getNumWorkers == numWorkers
      case Failure(exception) =>
        logger.error(s"Failure while settings $numWorkers workers: ${exception.getMessage}")
        false
    }*/

    // TODO: Better handle request results in an async way, using the answer from the workspace API
    //Idea is to store the answer, and check what is his result, in an syncronous way inside the isClusterScaling
    // method, in order to be sure on previous requests status. Databricks API still need to correctly document their
    // async interface (v 0.8.1)

    true
  }



  override def isClusterScaling: Boolean = {
    val workers = getWorkers.length
    val requested = getRequestedWorkers

    logger.info(s"Actual / Requested workers: ${workers} / $requested")
    requested != workers
  }

  def getClusterId(implicit sparkSession: SparkSession): String =
    sparkSession.conf.get(CONF_KEY_CLUSTERID)

  def getHostName(implicit sparkSession: SparkSession): String = {
    Try(sparkSession.conf.get(CONF_KEY_HOST)) match {
      case Success(value) => value
      case Failure(exception) =>
        logger.error("spark.databricks.hostName not defined on cluster. It must be defined as the workspace hostname for elastic scaling to work")
        "no-hostname"
    }
  }
}
