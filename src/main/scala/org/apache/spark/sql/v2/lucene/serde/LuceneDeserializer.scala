package org.apache.spark.sql.v2.lucene.serde

import org.apache.lucene.document.Document
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.lucene.serde.avro.StoreFieldAvroReader

class LuceneDeserializer(dataSchema: StructType,
                         requiredSchema: StructType,zoneId: String) {
  val storeFieldAvroReader=StoreFieldAvroReader(dataSchema,requiredSchema)
  def deserialize(doc: Document): InternalRow = {
    storeFieldAvroReader.deserialize(doc.getBinaryValue("_source").bytes).asInstanceOf[InternalRow]
  }
}
