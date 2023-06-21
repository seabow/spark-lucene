package io.github.seabow.spark.v2.lucene

import io.github.seabow.spark.v2.lucene.collector.PagingCollector
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.cache.{LuceneCacheAccumulator, LuceneSearcherCache}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, PartitionReaderWithPartitionValues}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.lucene.LuceneFilters
import org.apache.spark.sql.v2.lucene.serde.DocValuesColumnarBatchReader
import org.apache.spark.sql.v2.lucene.util.LuceneAggUtils
import org.apache.spark.sql.v3.evolving.expressions.aggregate.Aggregation
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

case class LucenePartitionReaderFactory(
       sqlConf: SQLConf,
      broadcastedConf: Broadcast[SerializableConfiguration],
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType,
      filters: Array[Filter],aggregation: Option[Aggregation],luceneCacheAccumulator:LuceneCacheAccumulator) extends FilePartitionReaderFactory{

  private val resultSchema = StructType(readDataSchema.fields ++ partitionSchema.fields)
  private val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled


  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val conf=broadcastedConf.value.value
    if(aggregation.nonEmpty){
      return buildReaderWithAggregation(file, conf)
    }

   val searcher=LuceneSearcherCache.getSearcherInstance(file.filePath,conf,luceneCacheAccumulator)
    val query = LuceneFilters.createFilter(dataSchema,filters)
    var currentPage=1
    var pagingCollector=new PagingCollector(currentPage,Int.MaxValue)
    searcher.search(query,pagingCollector)
    var docs= pagingCollector.docs
    val vectorizedReader=new DocValuesColumnarBatchReader(
      enableOffHeapColumnVector,
      searcher.getIndexReader,docs.toArray,
      readDataSchema,
      partitionSchema,
      null, capacity = 30000)

    val fileReader= new PartitionReader[InternalRow] {
      var rowIterator=vectorizedReader.columnarBatch.rowIterator()

      override def next(): Boolean = {
        rowIterator.hasNext||
          { if(vectorizedReader.nextBatch()){
            rowIterator=vectorizedReader.columnarBatch.rowIterator()
            true
          } else {
            false
          }
          }
      }

      override def get(): InternalRow = {
        //是否需要触发一次search?
        rowIterator.next()
      }

      override def close(): Unit = {
        vectorizedReader.close()
      }
    }
    new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
      partitionSchema, file.partitionValues)
  }

  def buildReaderWithAggregation(file: PartitionedFile,
                                 conf: Configuration):PartitionReader[InternalRow] ={
    val searcher=LuceneSearcherCache.getSearcherInstance(file.filePath,conf,luceneCacheAccumulator)
    val query = LuceneFilters.createFilter(dataSchema,filters)
    var internalRows=LuceneAggUtils.createAggInternalRows(aggregation.get,searcher,query,dataSchema,readDataSchema,partitionSchema,conf).iterator
    val fileReader= new PartitionReader[InternalRow] {
      override def next(): Boolean = {
        internalRows.hasNext
      }
      override def get(): InternalRow = {
        internalRows.next()
      }
      override def close(): Unit = {
        //DO Nothing
      }
    }
    fileReader
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = {
      sqlConf.wholeStageEnabled &&
      !WholeStageCodegenExec.isTooManyFields(sqlConf, resultSchema) &&
      aggregation.isEmpty
  }

  override def buildColumnarReader(partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val conf=broadcastedConf.value.value
    val searcher=LuceneSearcherCache.getSearcherInstance(partitionedFile.filePath,conf,luceneCacheAccumulator)
    val query = LuceneFilters.createFilter(dataSchema,filters)
    new PartitionReader[ColumnarBatch] {
      var currentPage=1
      var pagingCollector=new PagingCollector(currentPage,Int.MaxValue)
      searcher.search(query,pagingCollector)
      val docs= pagingCollector.docs
      val vectorizedReader=new DocValuesColumnarBatchReader(
        enableOffHeapColumnVector,
        searcher.getIndexReader,docs.toArray,
        readDataSchema,
        partitionSchema,
        partitionedFile.partitionValues, capacity = 30000)
      override def next(): Boolean = vectorizedReader.nextBatch()

      override def get(): ColumnarBatch =
        vectorizedReader.columnarBatch

      override def close(): Unit = vectorizedReader.close()
    }
  }
}
