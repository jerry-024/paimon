/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.write

import org.apache.paimon.CoreOptions
import org.apache.paimon.fs.CommittablePositionOutputStream
import org.apache.paimon.spark.{SparkInternalRowWrapper, SparkUtils}
import org.apache.paimon.table.{FileStoreTable, FormatTable, Table}
import org.apache.paimon.table.format.FormatBatchWriteBuilder
import org.apache.paimon.table.sink.{BatchTableWrite, BatchWriteBuilder, CommitMessage, CommitMessageSerializer}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType

import java.io.{IOException, UncheckedIOException}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class PaimonV2Write(
    storeTable: Table,
    overwriteDynamic: Boolean,
    overwritePartitions: Option[Map[String, String]],
    writeSchema: StructType
) extends Write
  with RequiresDistributionAndOrdering
  with Logging {

  assert(
    !(overwriteDynamic && overwritePartitions.exists(_.nonEmpty)),
    "Cannot overwrite dynamically and by filter both")

  private val table = {
    storeTable match {
      case fileStoreTable: FileStoreTable =>
        fileStoreTable.copy(
          Map(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key -> overwriteDynamic.toString).asJava)
      case _ => storeTable
    }
  }

  private val writeRequirement = PaimonWriteRequirement(table)

  override def requiredDistribution(): Distribution = {
    val distribution = writeRequirement.distribution
    logInfo(s"Requesting $distribution as write distribution for table ${table.name()}")
    distribution
  }

  override def requiredOrdering(): Array[SortOrder] = {
    val ordering = writeRequirement.ordering
    logInfo(s"Requesting ${ordering.mkString(",")} as write ordering for table ${table.name()}")
    ordering
  }

  override def toBatch: BatchWrite = {
    table match {
      case fileStoreTable: FileStoreTable =>
        PaimonBatchWrite(fileStoreTable, writeSchema, overwritePartitions)
      case formatTable: FormatTable =>
        FormatTableBatchWrite(formatTable, writeSchema, overwritePartitions)
    }
  }

  override def toString: String = {
    val overwriteDynamicStr = if (overwriteDynamic) {
      ", overwriteDynamic=true"
    } else {
      ""
    }
    val overwritePartitionsStr = overwritePartitions match {
      case Some(partitions) if partitions.nonEmpty => s", overwritePartitions=$partitions"
      case Some(_) => ", overwriteTable=true"
      case None => ""
    }
    s"PaimonWrite(table=${table.fullName()}$overwriteDynamicStr$overwritePartitionsStr)"
  }
}

private case class PaimonBatchWrite(
    table: FileStoreTable,
    writeSchema: StructType,
    overwritePartitions: Option[Map[String, String]])
  extends BatchWrite
  with WriteHelper {

  private val batchWriteBuilder = {
    val builder = table.newBatchWriteBuilder()
    overwritePartitions.foreach(partitions => builder.withOverwrite(partitions.asJava))
    builder
  }

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    WriterFactory(writeSchema, batchWriteBuilder)

  override def useCommitCoordinator(): Boolean = false

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"Committing to table ${table.name()}")
    val batchTableCommit = batchWriteBuilder.newCommit()

    val commitMessages = messages
      .collect {
        case taskCommit: TaskCommit => taskCommit.commitMessages()
        case other =>
          throw new IllegalArgumentException(s"${other.getClass.getName} is not supported")
      }
      .flatten
      .toSeq

    try {
      val start = System.currentTimeMillis()
      batchTableCommit.commit(commitMessages.asJava)
      logInfo(s"Committed in ${System.currentTimeMillis() - start} ms")
    } finally {
      batchTableCommit.close()
    }
    postCommit(commitMessages)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // TODO clean uncommitted files
  }
}

private case class WriterFactory(writeSchema: StructType, batchWriteBuilder: BatchWriteBuilder)
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val batchTableWrite = batchWriteBuilder.newWrite().asInstanceOf[BatchTableWrite[CommitMessage]]
    new PaimonDataWriter(batchTableWrite, writeSchema)
  }
}

private class PaimonDataWriter(
    batchTableWrite: BatchTableWrite[CommitMessage],
    writeSchema: StructType)
  extends DataWriter[InternalRow] {

  private val ioManager = SparkUtils.createIOManager()
  batchTableWrite.withIOManager(ioManager)

  private val rowConverter: InternalRow => SparkInternalRowWrapper = {
    val numFields = writeSchema.fields.length
    val reusableWrapper = new SparkInternalRowWrapper(-1, writeSchema, numFields)
    record => reusableWrapper.replace(record)
  }

  override def write(record: InternalRow): Unit = {
    batchTableWrite.write(rowConverter.apply(record))
  }

  override def commit(): WriterCommitMessage = {
    try {
      val commitMessages = batchTableWrite.prepareCommit().asScala.toSeq
      TaskCommit(commitMessages)
    } finally {
      close()
    }
  }

  override def abort(): Unit = close()

  override def close(): Unit = {
    try {
      batchTableWrite.close()
      ioManager.close()
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
  }
}

class TaskCommit private (
    private val serializedMessageBytes: Seq[Array[Byte]]
) extends WriterCommitMessage {
  def commitMessages(): Seq[CommitMessage] = {
    val deserializer = new CommitMessageSerializer()
    serializedMessageBytes.map {
      bytes =>
        Try(deserializer.deserialize(deserializer.getVersion, bytes)) match {
          case Success(msg) => msg
          case Failure(e: IOException) => throw new UncheckedIOException(e)
          case Failure(e) => throw e
        }
    }
  }
}

object TaskCommit {
  def apply(commitMessages: Seq[CommitMessage]): TaskCommit = {
    val serializer = new CommitMessageSerializer()
    val serializedBytes: Seq[Array[Byte]] = Option(commitMessages)
      .filter(_.nonEmpty)
      .map(_.map {
        msg =>
          Try(serializer.serialize(msg)) match {
            case Success(serializedBytes) => serializedBytes
            case Failure(e: IOException) => throw new UncheckedIOException(e)
            case Failure(e) => throw e
          }
      })
      .getOrElse(Seq.empty)

    new TaskCommit(serializedBytes)
  }
}

private case class FormatTableBatchWrite(
    table: FormatTable,
    writeSchema: StructType,
    overwritePartitions: Option[Map[String, String]])
  extends BatchWrite
  with Logging {

  private val batchWriteBuilder = {
    val builder = table.newBatchWriteBuilder().asInstanceOf[FormatBatchWriteBuilder]
    overwritePartitions.foreach(partitions => builder.withOverwrite(partitions.asJava))
    builder
  }

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    FormatTableWriterFactory(table, writeSchema, batchWriteBuilder)

  override def useCommitCoordinator(): Boolean = false

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"Committing to FormatTable ${table.name()}")

    // For FormatTable, we don't use the batch commit mechanism from the builder
    // Instead, we directly execute the committers
    val committers = messages
      .collect {
        case taskCommit: FormatTableTaskCommit => taskCommit.committers()
        case other =>
          throw new IllegalArgumentException(s"${other.getClass.getName} is not supported")
      }
      .flatten
      .toSeq

    try {
      val start = System.currentTimeMillis()
      committers.foreach(_.commit())
      logInfo(s"Committed in ${System.currentTimeMillis() - start} ms")
    } catch {
      case e: Exception =>
        logError("Failed to commit FormatTable writes", e)
        throw e
    }
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    logInfo(s"Aborting write to FormatTable ${table.name()}")
    // FormatTable doesn't have specific cleanup requirements for now
  }
}

private case class FormatTableWriterFactory(
    table: FormatTable,
    writeSchema: StructType,
    batchWriteBuilder: FormatBatchWriteBuilder)
  extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val formatTableWrite =
      batchWriteBuilder
        .newWrite()
        .asInstanceOf[BatchTableWrite[CommittablePositionOutputStream.Committer]]
    new FormatTableDataWriter(table, formatTableWrite, writeSchema)
  }
}

private class FormatTableDataWriter(
    table: FormatTable,
    formatTableWrite: BatchTableWrite[CommittablePositionOutputStream.Committer],
    writeSchema: StructType)
  extends DataWriter[InternalRow]
  with Logging {

  private val rowConverter: InternalRow => org.apache.paimon.data.InternalRow = {
    val numFields = writeSchema.fields.length
    record => {
      new SparkInternalRowWrapper(-1, writeSchema, numFields).replace(record)
    }
  }

  override def write(record: InternalRow): Unit = {
    val paimonRow = rowConverter.apply(record)
    formatTableWrite.write(paimonRow)
  }

  override def commit(): WriterCommitMessage = {
    try {
      val committers = formatTableWrite.prepareCommit().asScala.toSeq
      // Execute committers immediately to avoid serialization issues
      committers.foreach(_.commit())
      // Return empty commit message since we already committed
      FormatTableTaskCommit(Seq.empty)
    } finally {
      close()
    }
  }

  override def abort(): Unit = {
    logInfo("Aborting FormatTable data writer")
    close()
  }

  override def close(): Unit = {
    try {
      formatTableWrite.close()
    } catch {
      case e: Exception =>
        logError("Error closing FormatTableDataWriter", e)
        throw new RuntimeException(e)
    }
  }
}

/** Commit message container for FormatTable writes, holding committers that need to be executed. */
class FormatTableTaskCommit private (
    private val _committers: Seq[CommittablePositionOutputStream.Committer])
  extends WriterCommitMessage {

  def committers(): Seq[CommittablePositionOutputStream.Committer] = _committers
}

object FormatTableTaskCommit {
  def apply(committers: Seq[CommittablePositionOutputStream.Committer]): FormatTableTaskCommit = {
    new FormatTableTaskCommit(committers)
  }
}
