package com.amadeus.elastic_scaling.cluster_interface

import com.amadeus.elastic_scaling.{ClusterInterface, WorkerInfo}

case class NoOp() extends ClusterInterface () {
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
  override def getWorkers: Seq[WorkerInfo] = Nil

  override def getRequestedWorkers: Int = 0

  override def setWorkers(numWorkers: Int): Boolean = true
}
