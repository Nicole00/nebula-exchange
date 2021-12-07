/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.exchange.reader

import com.vesoft.nebula.exchange.config.{
  Configs,
  EdgeConfigEntry,
  KafkaSourceConfigEntry,
  PulsarSourceConfigEntry,
  SchemaConfigEntry,
  TagConfigEntry
}
import com.vesoft.nebula.exchange.processor.{EdgeProcessor, VerticesProcessor}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions.{col, concat_ws, get_json_object}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.util.LongAccumulator

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

  require(kafkaConfig.server.trim.nonEmpty && kafkaConfig.topics.nonEmpty)

  override def read(): DataFrame = {
    val df = session.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.server)
      .option("subscribe", kafkaConfig.topics(0))
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
//    if (kafkaConfig.vertexIdFileds.nonEmpty) {
//      data = data
//        .select(cols: _*)
//        .withColumn(kafkaConfig.vertexIdFileds.mkString("_"),
//                    col = concat_ws(":", kafkaConfig.vertexIdFileds.map(col): _*))
//
//    } else if (kafkaConfig.edgeSrcIdFields.nonEmpty) {
//      data = data
//        .select(cols: _*)
//        .withColumn(kafkaConfig.edgeSrcIdFields.mkString("_"),
//                    col = concat_ws(":", kafkaConfig.edgeSrcIdFields.map(col): _*))
//        .withColumn(kafkaConfig.edgeDstIdFIelds.mkString("_"),
//                    col = concat_ws(":", kafkaConfig.edgeDstIdFIelds.map(col): _*))
//    }
    data
  }
}

class KafkaReader1(override val session: SparkSession, kafkaConfig: KafkaSourceConfigEntry)
    extends StreamingBaseReader(session) {

  require(kafkaConfig.server.trim.nonEmpty && kafkaConfig.topics.nonEmpty)

  override def read(): DataFrame = ???

  def read1(tagConfigEntry: TagConfigEntry,
            edgeConfigEntry: EdgeConfigEntry,
            fieldKeys: List[String],
            nebulaKeys: List[String],
            config: Configs,
            batchSuccess: LongAccumulator,
            batchFailure: LongAccumulator): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers"  -> kafkaConfig.server,
      "key.deserializer"   -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id"           -> kafkaConfig.groupId,
      "auto.offset.reset"  -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val ssc = new StreamingContext(session.sparkContext, Seconds(kafkaConfig.intervalSeconds))

    var topics = new ListBuffer[String]
    for (topic <- kafkaConfig.topics) {
      topics.append(topic)
    }
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    // columns in kafka value
    val columns: ListBuffer[Column] = new ListBuffer[Column]
    for (field <- kafkaConfig.fields.distinct) {
      columns.append(get_json_object(col("value"), "$." + field).alias(field))
    }

    val schema = StructType(List(StructField("value", StringType, nullable = false)))

    stream.foreachRDD(rdd => {

      val value = rdd.map(row => Row(row.value()))
      val data = session.sqlContext
        .createDataFrame(value, schema)
        .selectExpr("CAST(value AS STRING)")
        .select(columns: _*)
        .na
        .drop(kafkaConfig.keyFields.distinct.size, kafkaConfig.keyFields.distinct)

      data.show()

      // tag import
      if (tagConfigEntry != null) {
        val process = new VerticesProcessor(tagConfigEntry,
                                            fieldKeys,
                                            nebulaKeys,
                                            config,
                                            batchSuccess,
                                            batchFailure)
        process.process(data)
      }

      // edge import
      if (edgeConfigEntry != null) {
        val process = new EdgeProcessor(edgeConfigEntry,
                                        fieldKeys,
                                        nebulaKeys,
                                        config,
                                        batchSuccess,
                                        batchFailure)
        process.process(data)
      }
    })
    ssc.start()
    ssc.awaitTermination()
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
