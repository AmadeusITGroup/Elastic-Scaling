package com.amadeus.elastic_scaling

import com.amadeus.elastic_scaling.config.ElasticConfig
import org.scalatest.{FlatSpec, Matchers}

class ElasticScalingTest
  extends FlatSpec
  with com.holdenkarau.spark.testing.DatasetSuiteBase
  with Matchers {
  
  behavior of "ElasticScalingTest"
  
  def elasticScaling() = {
    val configuredBatchDuration = 10000  // 10 s
  
    // threshold high = 1.0
    // threshold low = 0.75
    // executors high = 10
    // executors low = 2
    val elasticConf = ElasticConfig(true, 1.0, 0.75, 2, 10, 0.6)
    new ElasticScaling(configuredBatchDuration, spark, elasticConf)

  }
  
  it should "computeExecutorNumber (no change)" in {
    val meanOccupation = 0.8
    val currentExecutorNumber = 2
    val newExecutorNumber = elasticScaling().computeExecutorNumber(meanOccupation, currentExecutorNumber)
    newExecutorNumber shouldEqual 2
  }
  
  it should "computeExecutorNumber (add one)" in {
    val meanOccupation = 1.2
    val currentExecutorNumber = 2
    val newExecutorNumber = elasticScaling().computeExecutorNumber(meanOccupation, currentExecutorNumber)
    newExecutorNumber shouldEqual 3
  }
  
  it should "computeExecutorNumber (add more than one)" in {
    val meanOccupation = 2.2
    val currentExecutorNumber = 2
    val newExecutorNumber = elasticScaling().computeExecutorNumber(meanOccupation, currentExecutorNumber)
    newExecutorNumber shouldEqual 5
  }
  
  it should "computeExecutorNumber (add and hit threshold high limit)" in {
    val meanOccupation = 2.2
    val currentExecutorNumber = 8
    val newExecutorNumber = elasticScaling().computeExecutorNumber(meanOccupation, currentExecutorNumber)
    newExecutorNumber shouldEqual 10
  }
  
  it should "computeExecutorNumber (already at threshold high limit)" in {
    val meanOccupation = 2.2
    val currentExecutorNumber = 10
    val newExecutorNumber = elasticScaling().computeExecutorNumber(meanOccupation, currentExecutorNumber)
    newExecutorNumber shouldEqual 10
  }
  
  it should "computeExecutorNumber (reduce one)" in {
    val meanOccupation = 0.6
    val currentExecutorNumber = 3
    val newExecutorNumber = elasticScaling().computeExecutorNumber(meanOccupation, currentExecutorNumber)
    newExecutorNumber shouldEqual 2
  }
  
  it should "computeExecutorNumber (reduce more than one)" in {
    val meanOccupation = 0.4
    val currentExecutorNumber = 4
    val newExecutorNumber = elasticScaling().computeExecutorNumber(meanOccupation, currentExecutorNumber)
    newExecutorNumber shouldEqual 2
  }
  
  it should "computeExecutorNumber (reduce and hit threshold low limit)" in {
    val meanOccupation = 0.4
    val currentExecutorNumber = 3
    val newExecutorNumber = elasticScaling().computeExecutorNumber(meanOccupation, currentExecutorNumber)
    newExecutorNumber shouldEqual 2
  }
  
  it should "computeExecutorNumber (already at threshold low limit)" in {
    val meanOccupation = 0.4
    val currentExecutorNumber = 2
    val newExecutorNumber = elasticScaling().computeExecutorNumber(meanOccupation, currentExecutorNumber)
    newExecutorNumber shouldEqual 2
  }
  
  // we start above threshold high and no change is requested: we stay above
  it should "computeExecutorNumber (above threshold high, no change)" in {
    val meanOccupation = 0.9
    val currentExecutorNumber = 12
    val newExecutorNumber = elasticScaling().computeExecutorNumber(meanOccupation, currentExecutorNumber)
    newExecutorNumber shouldEqual 12
  }
  
  // we start above threshold high and we should increase: we get back in range
  it should "computeExecutorNumber (above threshold high, theoretical increase)" in {
    val meanOccupation = 1.1
    val currentExecutorNumber = 12
    val newExecutorNumber = elasticScaling().computeExecutorNumber(meanOccupation, currentExecutorNumber)
    newExecutorNumber shouldEqual 10
  }
  
  // we start below threshold low and no change is requested: we stay below
  it should "computeExecutorNumber (below threshold low, no change)" in {
    val meanOccupation = 0.9
    val currentExecutorNumber = 1
    val newExecutorNumber = elasticScaling().computeExecutorNumber(meanOccupation, currentExecutorNumber)
    newExecutorNumber shouldEqual 1
  }
  
  // we start below threshold low and we should decrease: we get back in range
  it should "computeExecutorNumber (below threshold low, theoretical decrease)" in {
    val meanOccupation = 0.5
    val currentExecutorNumber = 1
    val newExecutorNumber = elasticScaling().computeExecutorNumber(meanOccupation, currentExecutorNumber)
    newExecutorNumber shouldEqual 2
  }
  
}
