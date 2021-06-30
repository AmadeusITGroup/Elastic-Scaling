# Elastic Scaling for Spark

Elastic scaling is a library that allows to control the number of resources (executors) instantiated by a Spark
Structured Streaming Job in order to optimize the effective microbatch duration. 
The goal is to use the minimum needed resources for processing the workload, in a time that is the closest 
to the microbatch duration itself, adapting the number of executors to the volume of data received.

For example, if we have a configured microbatch duration of 1 minute, this library will try to adapt the number 
of executors so that the new data is processed in a time close (but less than) 1 minute.
Another advantage is that this will automatically scale the number of executors based on the evolution 
of the traffic data to be processed during the day, as that volume varies significantly. It will also allow to adapt to sudden changes in charge due to platform instability or recovery actions, lowering 
monitoring and job babysitting needs.

## Differences with Spark Dynamic Scaling
Spark dynamic scaling solves a different problem (apart from the fact that it should not really be used in a stream job):
it tries to use the maximum allowed resources based on the spark job parallelism in order to finish it in the smallest 
amount of time.
Elastic scaling tries to use the minimum number of resources needed to not lag behind in a streaming job.
This has several advantages, as it allows to avoid spikes in resource utilisation, smoothing overall CPU usage,
and facilitating scaling of cluster in a cloud environment.

## Inspiration
This work is inspired by https://databricks.com/session/auto-scaling-systems-with-elastic-spark-streaming. 
The motivation behind this contribution is to share a simple, yet efficient, 
library that offers the aforementioned capability. 

How to build
============

Run sbt package, then copy the resulting jarfile from target/scala-2.11 to your local package repository. 
Change the parameters in build.sbt if you need to modify the version of Scala and/or Spark you are compiling against. We tested it against Scala 2.11.8 and Spark 2.4.4


How to use
==========

First, define the properties [in square brackets the default value] in the spark conf:

    "com.amadeus.elastic_scaling.active" -> the library is activated or not -> true or [false]
    "com.amadeus.elastic_scaling.threshold.high" -> the workload multiplier above which the library requests new executors -> a Double value [1.0]
    "com.amadeus.elastic_scaling.threshold.low" -> the workload multipier below which the library releases executors -> a Double value [0.75]
    "com.amadeus.elastic_scaling.executors.max" -> the maximum number of executors to allocate to the job -> an Int value [10]
    "com.amadeus.elastic_scaling.executors.min" -> the minimum number of executors to allocate for the job -> an Int value [1]
    "com.amadeus.elastic_scaling.executors.smoothing_factor" -> the smoothing factor for mean occupation calculation -> a Double value [0.6] between (0.0, 1.0]

Example:

    spark.conf.set("com.amadeus.elastic_scaling.active", "true" )

Then, instantiate the ElasticScaling class:

```scala
val es = ElasticScaling(configuredBatchDuration, spark)
```
where:

    configuredBatchDuration -> The duration of the streaming microbatch (in ms)
    spark -> the Spark session

Finally, register the class within Spark

```scala
spark.streams.addListener(es)
```

The library will log the parameters at each iteration. 
If you need to modify this behavior, you have to override the showMetrics functions in ElasticScaling class