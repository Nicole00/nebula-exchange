/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.exchange.reader

import com.vesoft.nebula.exchange.config.{KafkaSourceConfigEntry, PulsarSourceConfigEntry}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, get_json_object}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Spark Streaming
  *
  * @param session
  */
abstract class StreamingBaseReader(override val session: SparkSession) extends Reader {

  override def close(): Unit = {
    session.close()
  }
}

/**
  *
  * @param session
  * @param kafkaConfig
  */
class KafkaReader(override val session: SparkSession, kafkaConfig: KafkaSourceConfigEntry)
    extends StreamingBaseReader(session) {

  require(kafkaConfig.server.trim.nonEmpty && kafkaConfig.topic.trim.nonEmpty)

  override def read(): DataFrame = {
    val df = session.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.server)
      .option("subscribe", kafkaConfig.topic)
      .load()
    val columns: ListBuffer[Column] = new ListBuffer[Column]
    for (field <- kafkaConfig.fields.distinct) {
      columns.append(get_json_object(col("value"), "$." + field).alias(field))
    }
    df.select(columns: _*)
  }
}

/**
  *
  * @param session
  * @param pulsarConfig
  */
class PulsarReader(override val session: SparkSession, pulsarConfig: PulsarSourceConfigEntry)
    extends StreamingBaseReader(session) {

  override def read(): DataFrame = {
    session.readStream
      .format("pulsar")
      .option("service.url", pulsarConfig.serviceUrl)
      .option("admin.url", pulsarConfig.adminUrl)
      .options(pulsarConfig.options)
      .load()
  }
}
