package io.github.seabow.spark.v2.lucene

import io.github.seabow.spark.v2.lucene.collector.PagingCollector
import org.apache.hadoop.conf.Configuration
import org.apache.lucene.document.Document
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.cache.{LuceneCacheAccumulator, LuceneSearcherCache}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReaderFactory, PartitionReaderWithPartitionValues}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.lucene.LuceneFilters
import org.apache.spark.sql.v2.lucene.serde.{DocValuesColumnarBatchReader, LuceneDeserializer}
import org.apache.spark.sql.v2.lucene.util.LuceneAggUtils
import org.apache.spark.sql.v3.evolving.expressions.aggregate.Aggregation
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String
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

    val deserializer=new LuceneDeserializer(dataSchema,readDataSchema,searcher.getIndexReader)

    val fileReader= new PartitionReader[InternalRow] {
      var currentPage=1
      var pagingCollector=new PagingCollector(currentPage,Int.MaxValue)
      searcher.search(query,pagingCollector)
      var iterator= pagingCollector.docs.iterator
      override def next(): Boolean = {
        if(iterator.hasNext) true else{
          if(pagingCollector.hasNextPage){
            currentPage=currentPage+1
            pagingCollector=new PagingCollector(currentPage,Int.MaxValue)
            searcher.search(query,pagingCollector)
            iterator=pagingCollector.docs.iterator
            true
          }else{
            false
          }
        }
      }

      override def get(): InternalRow = {
        //是否需要触发一次search?
        val doc=searcher.doc(iterator.next())
        deserializer.deserialize(doc)
      }

      override def close(): Unit = {
        //DO Nothing
      }
    }
    new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
      partitionSchema, file.partitionValues)
  }

  def convert(doc:Document,dataSchema:StructType):InternalRow={
    val length=dataSchema.length
    val resultRow = new SpecificInternalRow(dataSchema.map(_.dataType))
    for(idx<- 0 until(length)){
      val value=doc.get(dataSchema(idx).name)
      if(value==null){
        resultRow.setNullAt(idx)
      }else{
        dataSchema(idx).dataType match {
          case IntegerType =>
            resultRow.setInt(idx,doc.get(dataSchema(idx).name).toInt)
          case LongType =>
            resultRow.setLong(idx,doc.get(dataSchema(idx).name).toLong)
          case FloatType =>
            resultRow.setFloat(idx,doc.get(dataSchema(idx).name).toFloat)
          case DoubleType =>
            resultRow.setDouble(idx,doc.get(dataSchema(idx).name).toDouble)
          case StringType =>
            resultRow.update(idx, UTF8String.fromBytes(doc.get(dataSchema(idx).name).getBytes))
          case ArrayType(elementType, _) =>
            resultRow.update(idx,doc.getValues(dataSchema(idx).name))
          case _=>

        }
      }
    }
    resultRow
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

      override def close(): Unit = vectorizedReader.columnarBatch.close()
    }
  }
}
