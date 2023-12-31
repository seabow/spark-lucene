
/*
 * Copyright 2022 Martin Mauch (@nightscape)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasource.lucene

import io.github.seabow.spark.v2.lucene.collector.PagingLeafReaderStoreCollector
import io.github.seabow.spark.v2.lucene.{LuceneOptions, LuceneOutputWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.cache.LuceneSearcherCache
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.lucene.LuceneFilters
import org.apache.spark.sql.v2.lucene.serde.DocValuesColumnarBatchReader
import org.apache.spark.sql.v2.lucene.util.LuceneUtils
import org.apache.spark.util.SerializableConfiguration

import scala.collection.convert.ImplicitConversions._

/** derived from binary file data source. Needed to support writing Lucene using the V2 API
 */
class LuceneFileFormat extends FileFormat with DataSourceRegister {

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]
                          ): Option[StructType] = {
    LuceneUtils.inferSchema(sparkSession, files, options)
  }

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType
                           ): OutputWriterFactory = {
    val LuceneOptions = new LuceneOptions(options, sparkSession.sessionState.conf)

    new OutputWriterFactory {
      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        new LuceneOutputWriter(path, dataSchema, context, LuceneOptions)
      }

      override def getFileExtension(context: TaskAttemptContext): String =
        s".${LuceneOptions.fileExtension}"
    }
  }

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = {
    false
  }

  override def shortName(): String = "lucene"
  override def toString: String = "LUCENE"

  /*
  We need this class for writing only, thus reader is not implemented
   */
  override def buildReaderWithPartitionValues(
                                      sparkSession: SparkSession,
                                      dataSchema: StructType,
                                      partitionSchema: StructType,
                                      requiredSchema: StructType,
                                      filters: Seq[Filter],
                                      options: Map[String, String],
                                      hadoopConf: Configuration
                                    ): PartitionedFile => Iterator[InternalRow] = {
    val broadcastedConf = {
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    }
    val luceneCacheAccumulator=LuceneSearcherCache.registerLuceneCacheAccumulatorInstances(sparkSession)
    (file: PartitionedFile) => {
      val conf = broadcastedConf.value.value
      val searcher = LuceneSearcherCache.getSearcherInstance(file.filePath, conf,luceneCacheAccumulator)
      val query = LuceneFilters.createFilter(dataSchema, filters)
      var currentPage = 1
      var pagingCollector = new PagingLeafReaderStoreCollector(currentPage, Int.MaxValue)
      searcher.search(query, pagingCollector)
      var leafReaderStores = pagingCollector.getLeafReaderStores
      val vectorizedReader=new DocValuesColumnarBatchReader(
        false,
        searcher.getIndexReader,leafReaderStores,
        requiredSchema,
        partitionSchema,
        null, capacity = 30000)

      val fullSchema = requiredSchema.toAttributes ++ partitionSchema.toAttributes
      val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

      var iterator=vectorizedReader.columnarBatch.rowIterator().map{
        row=>
          unsafeProjection(row)
      }
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          iterator.hasNext||
            { if(vectorizedReader.nextBatch()){
              iterator=vectorizedReader.columnarBatch.rowIterator().map{
                row=>
                  unsafeProjection(row)
              }
              true
            } else {
              false
            }
            }

        }

        override def next(): InternalRow = {
          iterator.next()
        }
      }
    }
  }



}
