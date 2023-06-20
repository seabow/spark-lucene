package org.apache.spark.sql.v2.lucene.serde

import org.apache.lucene.document.Document
import org.apache.lucene.index.IndexReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.lucene.serde.avro.StoreFieldAvroReader

class LuceneDeserializer(dataSchema: StructType,
                         requiredSchema: StructType,indexReader:IndexReader) {
  val storeFieldAvroReader=StoreFieldAvroReader(dataSchema,requiredSchema)
  def deserialize(doc: Document): InternalRow = {
    storeFieldAvroReader.deserialize(doc.getBinaryValue("_source").bytes).asInstanceOf[InternalRow]
  }

//  def deserializer(docId:Int):InternalRow={
//    val leaves=indexReader.leaves()
//    println("查询结果顺序：")
//
//      val context = leaves.get(ReaderUtil.subIndex(docId, leaves))
//      val sortedSetDocValues = DocValues.getSortedSet(context.reader(),"myField")
//      sortedSetDocValues.advanceExact(docId)
//      Iterator.continually(sortedSetDocValues.nextOrd())
//        .takeWhile(_ != SortedSetDocValues.NO_MORE_ORDS)
//        .map(ordinal => sortedSetDocValues.lookupOrd(ordinal).utf8ToString()).foreach(println)
//  }
}
