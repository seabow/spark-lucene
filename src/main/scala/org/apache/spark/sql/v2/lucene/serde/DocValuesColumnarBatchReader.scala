package org.apache.spark.sql.v2.lucene.serde

import org.apache.lucene.index.IndexReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.{ColumnVectorUtils, OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class DocValuesColumnarBatchReader(enableOffHeapColumnVector:Boolean
                                   , indexReader:IndexReader,
                                   docIds:Array[Int],
                                   readDataSchema:StructType,
                                   partitionsSchema:StructType,
                                   partitionValues: InternalRow, capacity:Int=30000){
  val resultSchema=StructType(readDataSchema.fields ++ partitionsSchema.fields)
   val vectors: Seq[WritableColumnVector] = if (enableOffHeapColumnVector) {
    OffHeapColumnVector.allocateColumns(docIds.length, resultSchema)
  } else {
    OnHeapColumnVector.allocateColumns(docIds.length, resultSchema)
  }
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

  val vectorReaders:Seq[DocValuesColumnarVectorReader]= readDataSchema.map{
    f=> DocValuesColumnarVectorReader.makeReader(f.dataType)
  }

  var returnedData=0

  def nextBatch():Boolean={
    vectors.foreach(_.reset())
    columnarBatch.setNumRows(0)
    if(returnedData>=docIds.size){
     return  false
    }
    val batchDocIds=docIds.slice(returnedData,returnedData+capacity)
    for(i <- 0 until vectorReaders.length){
      vectorReaders(i).readBatch(indexReader:IndexReader,batchDocIds,readDataSchema(i).name,vectors(i))
    }
    returnedData=returnedData+batchDocIds.length
    columnarBatch.setNumRows(batchDocIds.length)
    true
  }


}
