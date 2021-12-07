/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.exchange

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File

import com.vesoft.nebula.exchange.config.{
  Configs,
  DataSourceConfigEntry,
  EdgeConfigEntry,
  KafkaSourceConfigEntry,
  SinkCategory,
  SourceCategory,
  TagConfigEntry
}
import com.vesoft.nebula.exchange.reader.KafkaReader1
import com.vesoft.nebula.exchange.processor.ReloadProcessor
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.util.LongAccumulator

final case class Argument(config: String = "application.conf",
                          hive: Boolean = false,
                          directly: Boolean = false,
                          dry: Boolean = false,
                          reload: String = "")

final case class TooManyErrorsException(private val message: String) extends Exception(message)

/**
  * SparkClientGenerator is a simple spark job used to write data into Nebula Graph parallel.
  */
object Exchange {
  private[this] val LOG = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val PROGRAM_NAME = "Nebula Graph Exchange"
    val options      = Configs.parser(args, PROGRAM_NAME)
    val c: Argument = options match {
      case Some(config) => config
      case _ =>
        LOG.error("Argument parse failed")
        sys.exit(-1)
    }

    val configs = Configs.parse(new File(c.config))
    LOG.info(s"Config ${configs}")

    val session = SparkSession
      .builder()
      .appName(PROGRAM_NAME)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    for (key <- configs.sparkConfigEntry.map.keySet) {
      session.config(key, configs.sparkConfigEntry.map(key))
    }

    val sparkConf = new SparkConf()
    sparkConf.registerKryoClasses(Array(classOf[com.facebook.thrift.async.TAsyncClientManager]))

    // config hive for sparkSession
    if (c.hive) {
      if (configs.hiveConfigEntry.isEmpty) {
        LOG.info("you don't config hive source, so using hive tied with spark.")
      } else {
        val hiveConfig = configs.hiveConfigEntry.get
        sparkConf.set("spark.sql.warehouse.dir", hiveConfig.warehouse)
        sparkConf
          .set("javax.jdo.option.ConnectionURL", hiveConfig.connectionURL)
          .set("javax.jdo.option.ConnectionDriverName", hiveConfig.connectionDriverName)
          .set("javax.jdo.option.ConnectionUserName", hiveConfig.connectionUserName)
          .set("javax.jdo.option.ConnectionPassword", hiveConfig.connectionPassWord)
      }
    }

    session.config(sparkConf)

    if (c.hive) {
      session.enableHiveSupport()
    }

    val spark = session.getOrCreate()

    // reload for failed import tasks
    if (!c.reload.isEmpty) {
      val batchSuccess = spark.sparkContext.longAccumulator(s"batchSuccess.reload")
      val batchFailure = spark.sparkContext.longAccumulator(s"batchFailure.reload")

      val data      = spark.read.text(c.reload)
      val processor = new ReloadProcessor(configs, batchSuccess, batchFailure)
      processor.process(data)
      LOG.info(s"batchSuccess.reload: ${batchSuccess.value}")
      LOG.info(s"batchFailure.reload: ${batchFailure.value}")
      sys.exit(0)
    }

    // record the failed batch number
    var failures: Long = 0L

    // import tags
    if (configs.tagsConfig.nonEmpty) {
      for (tagConfig <- configs.tagsConfig) {
        LOG.info(s"Processing Tag ${tagConfig.name}")

        val fieldKeys = tagConfig.fields
        LOG.info(s"field keys: ${fieldKeys.mkString(", ")}")
        val nebulaKeys = tagConfig.nebulaFields
        LOG.info(s"nebula keys: ${nebulaKeys.mkString(", ")}")
        val batchSuccess =
          spark.sparkContext.longAccumulator(s"batchSuccess.${tagConfig.name}")
        val batchFailure =
          spark.sparkContext.longAccumulator(s"batchFailure.${tagConfig.name}")

        createDataSource(spark,
                         tagConfig.dataSourceConfigEntry,
                         tagConfig,
                         null,
                         fieldKeys,
                         nebulaKeys,
                         batchSuccess,
                         batchFailure,
                         configs)
        val startTime = System.currentTimeMillis()
        val costTime  = ((System.currentTimeMillis() - startTime) / 1000.0).formatted("%.2f")
        LOG.info(s"import for tag ${tagConfig.name} cost time: ${costTime} s")
        if (tagConfig.dataSinkConfigEntry.category == SinkCategory.CLIENT) {
          LOG.info(s"Client-Import: batchSuccess.${tagConfig.name}: ${batchSuccess.value}")
          LOG.info(s"Client-Import: batchFailure.${tagConfig.name}: ${batchFailure.value}")
          failures += batchFailure.value
        } else {
          LOG.info(s"SST-Import: failure.${tagConfig.name}: ${batchFailure.value}")
        }
      }
    } else {
      LOG.warn("Tag is not defined")
    }

    // import edges
    if (configs.edgesConfig.nonEmpty) {
      for (edgeConfig <- configs.edgesConfig) {
        LOG.info(s"Processing Edge ${edgeConfig.name}")

        val fieldKeys = edgeConfig.fields
        LOG.info(s"field keys: ${fieldKeys.mkString(", ")}")
        val nebulaKeys = edgeConfig.nebulaFields
        LOG.info(s"nebula keys: ${nebulaKeys.mkString(", ")}")

        val batchSuccess = spark.sparkContext.longAccumulator(s"batchSuccess.${edgeConfig.name}")
        val batchFailure = spark.sparkContext.longAccumulator(s"batchFailure.${edgeConfig.name}")

        createDataSource(spark,
                         edgeConfig.dataSourceConfigEntry,
                         null,
                         edgeConfig,
                         fieldKeys,
                         nebulaKeys,
                         batchSuccess,
                         batchFailure,
                         configs)
        val startTime = System.currentTimeMillis()
        val costTime  = ((System.currentTimeMillis() - startTime) / 1000.0).formatted("%.2f")
        LOG.info(s"import for edge ${edgeConfig.name} cost time: ${costTime} s")
        if (edgeConfig.dataSinkConfigEntry.category == SinkCategory.CLIENT) {
          LOG.info(s"Client-Import: batchSuccess.${edgeConfig.name}: ${batchSuccess.value}")
          LOG.info(s"Client-Import: batchFailure.${edgeConfig.name}: ${batchFailure.value}")
          failures += batchFailure.value
        } else {
          LOG.info(s"SST-Import: failure.${edgeConfig.name}: ${batchFailure.value}")
        }
      }
    } else {
      LOG.warn("Edge is not defined")
    }

    // reimport for failed tags and edges
    if (failures > 0 && ErrorHandler.existError(configs.errorConfig.errorPath)) {
      val batchSuccess = spark.sparkContext.longAccumulator(s"batchSuccess.reimport")
      val batchFailure = spark.sparkContext.longAccumulator(s"batchFailure.reimport")
      val data         = spark.read.text(configs.errorConfig.errorPath)
      val startTime    = System.currentTimeMillis()
      val processor    = new ReloadProcessor(configs, batchSuccess, batchFailure)
      processor.process(data)
      val costTime = ((System.currentTimeMillis() - startTime) / 1000.0).formatted("%.2f")
      LOG.info(s"reimport ngql cost time: ${costTime}")
      LOG.info(s"batchSuccess.reimport: ${batchSuccess.value}")
      LOG.info(s"batchFailure.reimport: ${batchFailure.value}")
    }
    spark.close()
  }

  /**
    * Create data source for different data type.
    *
    * @param session The Spark Session.
    * @param config  The config.
    * @return
    */
  private[this] def createDataSource(
      session: SparkSession,
      config: DataSourceConfigEntry,
      tagConfig: TagConfigEntry,
      edgeConfig: EdgeConfigEntry,
      fieldKeys: List[String],
      nebulaKeys: List[String],
      batchSuccess: LongAccumulator,
      batchFailure: LongAccumulator,
      configs: Configs
  ): Unit = {
    config.category match {
      case SourceCategory.KAFKA => {
        val kafkaConfig = config.asInstanceOf[KafkaSourceConfigEntry]
        LOG.info(
          s"""Loading from Kafka ${kafkaConfig.server} and subscribe ${kafkaConfig.topics}""")
        val reader = new KafkaReader1(session, kafkaConfig)
        reader.read1(tagConfig,
                     edgeConfig,
                     fieldKeys,
                     nebulaKeys,
                     configs,
                     batchSuccess,
                     batchFailure)
      }

      case _ => {
        LOG.error(s"Data source ${config.category} not supported")
        None
      }
    }
  }

  /**
    * Repartition the data frame using the specified partition number.
    *
    * @param frame
    * @param partition
    * @return
    */
  private[this] def repartition(frame: DataFrame,
                                partition: Int,
                                sourceCategory: SourceCategory.Value): DataFrame = {
    if (partition > 0 && !CheckPointHandler.checkSupportResume(sourceCategory)) {
      frame.repartition(partition).toDF
    } else {
      frame
    }
  }
}
