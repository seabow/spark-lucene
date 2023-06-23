package org.apache.spark.sql.v2.lucene.serde

import org.apache.lucene.index._
import org.apache.lucene.util.NumericUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.execution.vectorized.WritableColumnVector
import org.apache.spark.sql.types._

import scala.collection.mutable

object DocValuesColumnarVectorReader {
  def makeReader(dataType: DataType): DocValuesColumnarVectorReader = {
    dataType match {
      case BooleanType =>
        new BooleanReader
      case DateType | IntegerType =>
        new IntReader
      case TimestampType | LongType =>
        new LongReader
      case FloatType =>
        new FloatReader
      case DoubleType =>
        new DoubleReader
      case StringType=>
        new StringReader
      case MapType(keyType, valueType, _) =>
        new MapReader(MultiDocValuesReader(makeReader(keyType)), makeReader(valueType))
      case st:StructType=>
        new StructReader(st,st.fields.map(f=>makeReader(f.dataType)))
      case ArrayType(elementType,_)=>
        new ArrayReader(makeReader(elementType))
    }
  }
}

abstract class DocValuesColumnarVectorReader {
  def readBatch(leafReader:LeafReader, docs:Array[Int], name: String, vector: WritableColumnVector): Unit = {
    throw new IllegalStateException("Unsupported")
  }

  def getValue(leafReader:LeafReader, docId: Int, name: String): Option[Any] = {
    throw new IllegalStateException("Unsupported")
  }

  def convert(fromValue: Any): Any = {
    throw new IllegalStateException("Unsupported")
  }

  def append(value: Any, vector: WritableColumnVector): Unit = {
    throw new IllegalStateException("Unsupported")
  }
}


abstract class NumericValuesReader extends DocValuesColumnarVectorReader {
  var numericDocValuesMap: mutable.Map[String, NumericDocValues] = mutable.Map.empty

  override def readBatch(leafReader:LeafReader, docs:Array[Int], name: String, vector: WritableColumnVector): Unit = {
    for (docId <- docs) {
      val value = getValue(leafReader, docId, name)
      if (value.isDefined) {
        append(value.get, vector)
      } else {
        vector.appendNull()
      }
    }
  }

  override def getValue(leafReader:LeafReader, docId: Int, name: String): Option[Any] = {
    if (!numericDocValuesMap.contains(name)) {
      numericDocValuesMap.put(name, leafReader.getNumericDocValues( name))
    }
    val numericDocValues = numericDocValuesMap(name)
    val value = if (numericDocValues!=null && numericDocValues.advanceExact(docId)) {
      val longValue=numericDocValues.longValue()
      Some(convert(longValue))
    } else {
      None
    }
    value
  }

}

class BooleanReader extends NumericValuesReader {
  override def convert(fromValue: Any): Any = {
    fromValue.asInstanceOf[Long] > 0
  }

  override def append(value: Any, vector: WritableColumnVector): Unit = {
    vector.appendBoolean(value.asInstanceOf[Boolean])
  }
}

class LongReader extends NumericValuesReader {
  override def convert(fromValue: Any): Any = {
    fromValue.asInstanceOf[Long]
  }

  override def append(value: Any, vector: WritableColumnVector): Unit = {
    vector.appendLong(value.asInstanceOf[Long])
  }
}

class IntReader extends NumericValuesReader {
  override def convert(fromValue: Any): Any = {
    fromValue.asInstanceOf[Long].toInt
  }

  override def append(value: Any, vector: WritableColumnVector): Unit = {
    vector.appendInt(value.asInstanceOf[Int])
  }
}

class FloatReader extends NumericValuesReader {
  override def convert(fromValue: Any): Any = {
    NumericUtils.sortableIntToFloat(fromValue.asInstanceOf[Long].toInt)
  }

  override def append(value: Any, vector: WritableColumnVector): Unit = {
    vector.appendFloat(value.asInstanceOf[Float])
  }
}


class DoubleReader extends NumericValuesReader {
  override def convert(fromValue: Any): Any = {
    NumericUtils.sortableLongToDouble(fromValue.asInstanceOf[Long])
  }

  override def append(value: Any, vector: WritableColumnVector): Unit = {
    vector.appendDouble(value.asInstanceOf[Double])
  }
}

class StringReader extends DocValuesColumnarVectorReader {
  var binaryDocValuesMap: mutable.Map[String, BinaryDocValues] = mutable.Map.empty

  override def readBatch(leafReader: LeafReader, batchDocIds: Array[Int], name: String, vector: WritableColumnVector): Unit = {
    for (docId <- batchDocIds) {
      val value = getValue(leafReader, docId, name)
      if (value.isDefined) {
        val valueBytes = value.get.asInstanceOf[String].getBytes()
        vector.appendByteArray(valueBytes, 0, valueBytes.length)
      } else {
        vector.appendNull()
      }
    }
  }

  override def convert(fromValue: Any): Any = {
    fromValue
  }

  override def getValue(leafReader: LeafReader, docId: Int, name: String): Option[Any] = {
    if (!binaryDocValuesMap.contains(name)) {
      binaryDocValuesMap.put(name, leafReader.getBinaryDocValues( name))
    }
    val binaryDocValues = binaryDocValuesMap(name)
    val value = if (binaryDocValues!=null && binaryDocValues.advanceExact(docId)) {
      val strValue=binaryDocValues.binaryValue().utf8ToString()
      Some(strValue)
    } else {
      None
    }
    value
  }

  override def append(value: Any, vector: WritableColumnVector): Unit = {
    val valueBytes = value.asInstanceOf[String].getBytes
    vector.appendByteArray(valueBytes, 0, valueBytes.length)
  }
}


class StructReader(structType: StructType, childConverters: Array[DocValuesColumnarVectorReader]) extends DocValuesColumnarVectorReader {
  var binaryDocValuesOption: Option[BinaryDocValues] = None

  override def readBatch(leafReader: LeafReader, batchDocIds: Array[Int], name: String, vector: WritableColumnVector): Unit = {
    if (binaryDocValuesOption.isEmpty) {
      binaryDocValuesOption = Some(leafReader.getBinaryDocValues( name))
    }
    val binaryDocValues = binaryDocValuesOption.get
    for (i <- 0 until batchDocIds.length) {
      if (binaryDocValues!=null && binaryDocValues.advanceExact(batchDocIds(i))) {
        vector.appendStruct(false)
        for (j <- 0 until childConverters.length) {
          childConverters(j).readBatch(leafReader, Array(batchDocIds(i)), Array(name, structType(j).name).quoted, vector.getChild(j))
        }
      } else {
        vector.appendStruct(true)
      }
    }
  }
}

object MultiDocValuesReader{
  def apply(childReader: DocValuesColumnarVectorReader): MultiDocValuesReaderBase = childReader match {
    case numerReader: NumericValuesReader =>
      new NumericMultiValueReader(numerReader)
    case stringReader: StringReader =>
      new StringMultiValueReader(stringReader)
    case _=>new MultiDocValuesReaderBase(childReader)
  }
}

class MultiDocValuesReaderBase(childReader: DocValuesColumnarVectorReader) extends DocValuesColumnarVectorReader {
  var numericDocValuesMap: mutable.Map[String,NumericDocValues]=mutable.Map.empty
  override def append(value: Any, vector: WritableColumnVector): Unit = {
    childReader.append(value, vector)
  }
  def getSize(indexReader:IndexReader, docId: Int, name: String): Option[Long] ={
    val sizeDocValues=getSizeDocValues(indexReader, name)
    if(sizeDocValues==null || !sizeDocValues.advanceExact(docId)){
       None
    }else {
       Some(sizeDocValues.longValue())
    }
  }

  def getSizeDocValues(indexReader: IndexReader, name: String): NumericDocValues = {
    val sizeFieldName = Array(name, "`size`").quoted
    if (!numericDocValuesMap.contains(sizeFieldName)) {
      numericDocValuesMap.put(sizeFieldName, MultiDocValues.getNumericValues(indexReader, sizeFieldName))
    }
    numericDocValuesMap(sizeFieldName)
  }

}

class StringMultiValueReader(childReader: DocValuesColumnarVectorReader) extends MultiDocValuesReaderBase(childReader) {
  var sortedSetDocValuesMap: mutable.Map[String, SortedSetDocValues] = mutable.Map.empty

  override def getValue(leafReader: LeafReader, docId: Int, name: String): Option[Any] = {
    val size=getSize(leafReader, docId, name)
    if(size.isEmpty){
      return None
    }else if (size.get==0){
      return Some(Seq.empty)
    }
    if (!sortedSetDocValuesMap.contains(name)) {
      sortedSetDocValuesMap.put(name, leafReader.getSortedSetDocValues( name))
    }
    val sortedSetDocValues = sortedSetDocValuesMap(name)
    if (sortedSetDocValues!=null && sortedSetDocValues.advanceExact(docId)) {
      val collection = Iterator.continually(sortedSetDocValues.nextOrd())
        .takeWhile(_ != SortedSetDocValues.NO_MORE_ORDS)
        .map { ordinal =>
         sortedSetDocValues.lookupOrd(ordinal).utf8ToString()
        }.toSeq
      Some(collection)
    } else {
      None
    }
  }
}

class NumericMultiValueReader(childReader: DocValuesColumnarVectorReader) extends MultiDocValuesReaderBase(childReader) {
  var sortedNumericDocValuesMap: mutable.Map[String, SortedNumericDocValues] = mutable.Map.empty

  override def getValue(leafReader: LeafReader, docId: Int, name: String): Option[Any] = {
    val size=getSize(leafReader, docId, name)
    if(size.isEmpty){
      return None
    }else if (size.get==0){
      return Some(Seq.empty)
    }
    if (!sortedNumericDocValuesMap.contains(name)) {
      sortedNumericDocValuesMap.put(name, leafReader.getSortedNumericDocValues( name))
    }
    val sortedNumericDocValues = sortedNumericDocValuesMap(name)
    if (sortedNumericDocValues!=null && sortedNumericDocValues.advanceExact(docId)) {
      val collection = for (j <- 0 until sortedNumericDocValues.docValueCount()) yield {
        sortedNumericDocValues.nextValue()
      }
      Some(collection.map(childReader.convert))
    } else {
      None
    }
  }
}

class MapReader(keyReader: DocValuesColumnarVectorReader, valueReader: DocValuesColumnarVectorReader) extends DocValuesColumnarVectorReader {
  val multiDocValuesReader=MultiDocValuesReader(keyReader)
  override def readBatch(leafReader: LeafReader, batchDocIds: Array[Int], name: String, vector: WritableColumnVector): Unit = {
    val keysVector = vector.getChild(0)
    val valuesVector = vector.getChild(1)
    for (i <- 0 until batchDocIds.length) {
      val keys = keyReader.getValue(leafReader, batchDocIds(i), name)
      if (keys.isDefined) {
        val keySeq = keys.get.asInstanceOf[Seq[Any]]
        val size = keySeq.size
        vector.appendArray(size)
        keySeq.foreach {
          key =>
            val keyString=key.toString
            keyReader.append(key, keysVector)
            valueReader.readBatch(leafReader, Array(batchDocIds(i)), Array(name, keyString).quoted, valuesVector)
        }
      } else {
        vector.appendNull()
      }
    }
  }


}

class ArrayReader(elementReader:DocValuesColumnarVectorReader) extends DocValuesColumnarVectorReader{
  val multiDocValuesReader=MultiDocValuesReader(elementReader)
  override def readBatch(leafReader:LeafReader, batchDocIds: Array[Int], name: String, vector: WritableColumnVector): Unit = {
    val numericDocValues=multiDocValuesReader.getSizeDocValues(leafReader, name)
    if(numericDocValues==null){
      vector.appendNulls(batchDocIds.length)
      return
    }

    for(i <- 0 until batchDocIds.length){
      if(numericDocValues.advanceExact(batchDocIds(i))){
        val numElements = numericDocValues.longValue().toInt
        vector.appendArray(numElements)
        val arrayVector=vector.arrayData()
        for(j <- 0 until numElements){
          elementReader.readBatch(leafReader,Array(batchDocIds(i)),s"$name[$j]",arrayVector)
        }
      }else{
        vector.appendNull()
      }
    }
  }

}