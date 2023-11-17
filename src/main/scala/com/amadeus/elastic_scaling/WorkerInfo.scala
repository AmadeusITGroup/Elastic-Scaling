package com.amadeus.elastic_scaling

/**
 * Defines a WorkerInfo case class for storing information about a worker in a Spark cluster.
 *
 * @param id   The unique identifier of the worker.
 * @param name The name or label associated with the worker.
 */
case class WorkerInfo(id: String, name: String)
