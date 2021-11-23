/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.exchange.utils

import java.io.File
import java.nio.charset.Charset
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import scala.io.Source

object HDFSUtils {
  private[this] val LOG = Logger.getLogger(this.getClass)

  def getFileSystem(namenode: String = null): FileSystem = {
    val conf = new Configuration()
    if (namenode != null) {
      conf.set("fs.default.name", namenode)
      conf.set("fs.defaultFS", namenode)
    }
    FileSystem.get(conf)
  }

  def list(path: String): List[String] = {
    val system = getFileSystem()
    system.listStatus(new Path(path)).map(_.getPath.getName).toList
  }

  def exists(path: String): Boolean = {
    val system = getFileSystem()
    system.exists(new Path(path))
  }

  def getContent(path: String): String = {
    val system      = getFileSystem()
    val inputStream = system.open(new Path(path))
    Source.fromInputStream(inputStream).mkString
  }

  def saveContent(path: String,
                  content: String,
                  charset: Charset = Charset.defaultCharset()): Unit = {
    val system       = getFileSystem()
    val outputStream = system.create(new Path(path))
    try {
      outputStream.write(content.getBytes(charset))
    } finally {
      outputStream.close()
    }
  }

  def upload(localPath: String, remotePath: String, namenode: String = null): Unit = {
    LOG.info("update sst file to hdfs")
    try {
      val localFile = new File(localPath)
      if (!localFile.exists() || localFile.length() <= 0) {
        return
      }
    } catch {
      case e: Throwable =>
        LOG.warn("check for empty local file error, but you can ignore this check error. " +
                   "If there is empty sst file in your hdfs, please delete it manually",
                 e)
    }
    val system = getFileSystem(namenode)
    system.copyFromLocalFile(new Path(localPath), new Path(remotePath))
  }
}
