/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.exchange.reader

import com.vesoft.nebula.exchange.config.{KafkaSourceConfigEntry, PulsarSourceConfigEntry}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, concat_ws, get_json_object}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}

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
      .option("startingOffsets", "earliest")
      .load()

    val columns: ListBuffer[Column] = new ListBuffer[Column]
    for (field <- kafkaConfig.fields.distinct) {
      columns.append(get_json_object(col("value"), "$." + field).alias(field))
    }

    var data = df
      .selectExpr("CAST(value AS STRING)")
      .as[String](Encoders.STRING)
//      .select(get_json_object(col("value"), "$.data").alias("data"))
      .select(columns: _*)
      .na
      .drop(kafkaConfig.keyFields.distinct.size, kafkaConfig.keyFields.distinct)

    val cols: List[Column] = data.schema.fields.toList.map(column => col(column.name))
    if (kafkaConfig.vertexIdFileds.nonEmpty) {
      data = data
        .select(cols: _*)
        .withColumn(kafkaConfig.vertexIdFileds.mkString("_"),
                    col = concat_ws(":", kafkaConfig.vertexIdFileds.map(col): _*))

    } else if (kafkaConfig.edgeSrcIdFields.nonEmpty) {
      data = data
        .select(cols: _*)
        .withColumn(kafkaConfig.edgeSrcIdFields.mkString("_"),
                    col = concat_ws(":", kafkaConfig.edgeSrcIdFields.map(col): _*))
        .withColumn(kafkaConfig.edgeDstIdFIelds.mkString("_"),
                    col = concat_ws(":", kafkaConfig.edgeDstIdFIelds.map(col): _*))
    }
    data
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
