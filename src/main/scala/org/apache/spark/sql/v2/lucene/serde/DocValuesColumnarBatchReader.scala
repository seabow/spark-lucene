package org.apache.spark.sql.v2.lucene.serde

import io.github.seabow.spark.v2.lucene.collector.LeafReaderStore
import org.apache.lucene.index.IndexReader
import org.apache.lucene.search.DocIdSetIterator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.{ColumnVectorUtils, OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.io.IOException
import scala.collection.mutable.ListBuffer

class DocValuesColumnarBatchReader(enableOffHeapColumnVector:Boolean
                                   , indexReader:IndexReader,
                                   leafReaderStores: ListBuffer[LeafReaderStore],
                                   readDataSchema:StructType,
                                   partitionsSchema:StructType,
                                   partitionValues: InternalRow, capacity:Int=30000){
  val resultSchema=StructType(readDataSchema.fields ++ partitionsSchema.fields)
   val vectors: Seq[WritableColumnVector] = if (enableOffHeapColumnVector) {
    OffHeapColumnVector.allocateColumns(capacity, resultSchema)
  } else {
    OnHeapColumnVector.allocateColumns(capacity, resultSchema)
  }
  val totalSize={
    leafReaderStores.map(_.size).sum
  }
  val leafReaderStoresIterator = leafReaderStores.iterator
  var currentLeafReaderStore :LeafReaderStore=_
  var currentDocIdSetIterator:DocIdSetIterator=_
  val columnarBatch: ColumnarBatch = {
    val cb=new ColumnarBatch(vectors.toArray)
    if (partitionsSchema != null) {
      val partitionIdx = readDataSchema.fields.length
      for (i <- 0 until partitionsSchema.fields.length) {
        ColumnVectorUtils.populate(vectors(i + partitionIdx), partitionValues, i)
        vectors(i + partitionIdx).setIsConstant()
      }
    }
    cb
  }

  var vectorReaders:Seq[DocValuesColumnarVectorReader]= readDataSchema.map{
    f=> DocValuesColumnarVectorReader.makeReader(f.dataType)
  }

  var returnedData=0
  var totalCountLoadedSoFar:Int = 0

  def nextBatch():Boolean={
    vectors.foreach(_.reset())
    columnarBatch.setNumRows(0)
    if(returnedData>=totalSize){
     return false
    }
    checkEndOfLeafReader()
    val num = Math.min(capacity, totalCountLoadedSoFar - returnedData)
   val docIds= for(i <- 0 until num)yield {currentDocIdSetIterator.nextDoc()}
    for(i <- 0 until vectorReaders.length){
      vectorReaders(i).readBatch(currentLeafReaderStore.context.reader(),docIds.toArray,readDataSchema(i).name,vectors(i))
    }
    returnedData+=num
    columnarBatch.setNumRows(num)
    true
  }

  def close():Unit={
    columnarBatch.close()
  }

  private def checkEndOfLeafReader(): Unit = {
    if (returnedData != totalCountLoadedSoFar) {
      return
    }
    val leafReaderStore: LeafReaderStore = leafReaderStoresIterator.next()
    currentLeafReaderStore=leafReaderStore
    currentDocIdSetIterator=leafReaderStore.docIdSet.iterator()
    if (leafReaderStore == null) {
      throw new IOException("expecting more rows but reached last block. Read " + returnedData + " out of " + totalSize)
    }
    totalCountLoadedSoFar += leafReaderStore.size
  }
}
