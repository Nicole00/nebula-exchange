/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.exchange.reader

import com.vesoft.nebula.exchange.config.{EdgeConfigEntry, KafkaSourceConfigEntry, TagConfigEntry}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{Column, DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat_ws, get_json_object}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

class KafkaReader(override val session: SparkSession,
                  kafkaConfig: KafkaSourceConfigEntry,
                  isTag: Boolean,
                  tagConfig: TagConfigEntry,
                  edgeConfig: EdgeConfigEntry)
    extends StreamingBaseReader(session) {

  require(kafkaConfig.server.trim.nonEmpty && kafkaConfig.topic.trim.nonEmpty)

  override def read(): DataFrame = {
    val sparkConf = session.sparkContext
    val ssc       = new StreamingContext(sparkConf, Seconds(10))
    val kafkaParams = scala.collection
      .Map[String, Object](
        "bootstrap.servers"             -> kafkaConfig.server,
        "key.deserializer"              -> classOf[StringDeserializer],
        "value.deserializer"            -> classOf[StringDeserializer],
        "group.id"                      -> kafkaConfig.groupId,
        "auto.offset.reset"             -> "latest",
        "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor",
        "enable.auto.commit"            -> (true: java.lang.Boolean)
      )
      .asJava

    val topics = new java.util.ArrayList[java.lang.String](1)
    topics.add(kafkaConfig.topic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD(rdd => {
      val rowRdd = rdd.map(row => Row(row.key(), row.value()))
      val schema = StructType(
        List(
          StructField("key", StringType, nullable = false),
          StructField("value", StringType, nullable = false)
        ))
      val dataFrame                   = session.sqlContext.createDataFrame(rowRdd, schema)
      val columns: ListBuffer[Column] = new ListBuffer[Column]
      for (field <- kafkaConfig.fields.distinct) {
        columns.append(get_json_object(col("value"), "$." + field).alias(field))
      }

      var data = dataFrame
        .selectExpr("CAST(value AS STRING)")
        .as[String](Encoders.STRING)
        //      .select(get_json_object(col("value"), "$.data").alias("data"))
        .select(columns: _*)
        .na
        .drop(kafkaConfig.keyFields.distinct.size, kafkaConfig.keyFields.distinct)

      // https://www.codeleading.com/article/8108619463/
      if (isTag) {
        data
          .dropDuplicates(tagConfig.vertexField)
          .mapPartitions(func)(Encoders.tuple(Encoders.BINARY, Encoders.BINARY))
          .toDF("key", "value")
          .sortWithinPartitions("key")
          .foreachPartition(func)
      } else {
        val distinctData = if (edgeConfig.rankingField.isDefined) {
          data.dropDuplicates(edgeConfig.sourceField,
                              edgeConfig.targetField,
                              edgeConfig.rankingField.get)
        } else {
          data.dropDuplicates(edgeConfig.sourceField, edgeConfig.targetField)
        }
        distinctData
          .mapPartitions(func)(Encoders.tuple(Encoders.BINARY, Encoders.BINARY))
          .toDF("key", "value")
          .sortWithinPartitions("key")
          .foreachPartition(func)
      }

    })
  }
}
