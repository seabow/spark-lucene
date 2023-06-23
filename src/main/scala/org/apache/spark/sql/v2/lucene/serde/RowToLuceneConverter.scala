package org.apache.spark.sql.v2.lucene.serde

import org.apache.lucene.document._
import org.apache.lucene.util.{BytesRef, NumericUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.types._

object RowToLuceneConverter {
  def makeConverter(dataType: DataType): RowToLuceneConverter = {
    dataType match {
      case BooleanType =>
        new BooleanConverter
      case DateType | IntegerType =>
        new IntegerConverter
      case TimestampType | LongType =>
        new LongConverter
      case FloatType =>
        new FloatConverter
      case DoubleType =>
        new DoubleConverter
      case StringType=>
        new StringConverter
      case MapType(keyType, valueType, _) =>
        new MapConverter(makeConverter(keyType), makeConverter(valueType))
      case st:StructType=>
        new StructConverter(st,st.fields.map(f=>makeConverter(f.dataType)))
      case ArrayType(elementType,_)=>
        new ArrayConverter(makeConverter(elementType))
    }
  }
}

abstract class RowToLuceneConverter{
  def appendSize(size: Long, name: String, doc: Document):Unit={
    doc.add(new NumericDocValuesField(Array(name,"`size`").quoted,size))
  }
  def appendFieldName( name: String, doc: Document):Unit={
    doc.add(new StringField( "_field_names",name, Field.Store.NO))
  }
  def appendFieldNameDocValue( name: String, doc: Document):Unit={
    doc.add(new BinaryDocValuesField(name, new BytesRef()))
  }
  def append(row:SpecializedGetters, ordinal:Int, name:String, doc:Document):Unit = {
    val value =getValue(row,ordinal)
    appendIndex(value,name,doc)
    appendDocValues(value,name,doc)
  }
   def appendIndex(value: Any, name: String, doc: Document): Unit
   def appendDocValues(value: Any, name: String, doc: Document): Unit
   def getValue(row: SpecializedGetters, ordinal: Int): Any
}

abstract class NumericBasedConverter extends RowToLuceneConverter{
  override def appendDocValues(value:Any, name:String,  doc: Document):Unit={
    val docValue= new NumericDocValuesField(name,valueToLong(value))
    doc.add(docValue)
  }

  override def append(row: SpecializedGetters, ordinal: Int, name: String, doc: Document): Unit = {
    val value =getValue(row,ordinal)
    appendIndex(value,name,doc)
    appendDocValues(value,name,doc)
  }

  def valueToLong(value:Any):Long={
    value.asInstanceOf[Long]
  }
}

class IntegerConverter extends NumericBasedConverter{
  override def appendIndex(value:Any, name: String, doc: Document): Unit = {
    doc.add(new IntPoint(name, value.asInstanceOf[Int]))
  }
  override def getValue(row: SpecializedGetters, ordinal: Int): Any = {
    row.getInt(ordinal)
  }
  override def valueToLong(value:Any):Long={
    value.asInstanceOf[Int].toLong
  }
}

class BooleanConverter extends IntegerConverter{
  override def getValue(row: SpecializedGetters, ordinal:Int):Any={
    if(row.getBoolean(ordinal)) 1 else 0
  }
}

class LongConverter extends NumericBasedConverter{
  override def appendIndex(value:Any, name: String, doc: Document): Unit = {
    doc.add(new LongPoint(name, value.asInstanceOf[Long]))
  }
  override def getValue(row: SpecializedGetters, ordinal: Int): Any = {
    row.getLong(ordinal)
  }
}

class FloatConverter extends NumericBasedConverter{
  override def appendIndex(value:Any, name: String, doc: Document): Unit = {
    doc.add(new FloatPoint(name, value.asInstanceOf[Float]))
  }

  override def valueToLong(value: Any): Long = {
    NumericUtils.floatToSortableInt(value.asInstanceOf[Float])
  }

  override def getValue(row: SpecializedGetters, ordinal: Int): Any = {
    row.getFloat(ordinal)
  }
}

class DoubleConverter extends NumericBasedConverter{
  override def appendIndex(value:Any, name: String, doc: Document): Unit = {
    doc.add(new DoublePoint(name, value.asInstanceOf[Double]))
  }

  override def valueToLong(value: Any): Long = {
    NumericUtils.doubleToSortableLong(value.asInstanceOf[Double])
  }

  override def getValue(row: SpecializedGetters, ordinal: Int): Any = {
    row.getDouble(ordinal)
  }
}

class StringConverter extends RowToLuceneConverter{
  override def appendIndex(value: Any, name: String, doc: Document): Unit = {
    doc.add(new StringField(name,value.toString, Field.Store.NO))
  }

  override def appendDocValues(value: Any, name: String, doc: Document): Unit = {
    val docValue=new BinaryDocValuesField(name, new BytesRef(value.toString))
    doc.add(docValue)
  }

  override def getValue(row: SpecializedGetters, ordinal: Int): Any = {
    row.getUTF8String(ordinal).toString
  }
}

object MultiValueConverter{
  def apply(elementConverter:RowToLuceneConverter): MultiValueConverterBase = {
    elementConverter match {
      case converter:NumericBasedConverter=>
        new NumericMultiValueConverter(converter)
      case converter:StringConverter=>
        new StringMultiValueConverter(converter)
      case _=> new ComplexMultiValueConverter(elementConverter)
    }
  }
}


abstract class MultiValueConverterBase(elementConverter:RowToLuceneConverter) extends RowToLuceneConverter{

  override def appendIndex(value: Any, name: String, doc: Document): Unit = {
    elementConverter.appendIndex(value, name, doc)
  }
  override def getValue(row: SpecializedGetters, ordinal: Int): Any = {
    elementConverter.getValue(row,ordinal)
  }
}

class StringMultiValueConverter(elementConverter:RowToLuceneConverter) extends MultiValueConverterBase(elementConverter){
  override def appendDocValues(value: Any, name: String, doc: Document): Unit = {
    val docValue=new SortedSetDocValuesField(name, new BytesRef(value.asInstanceOf[String]))
    doc.add(docValue)
  }
}

class NumericMultiValueConverter(elementConverter:RowToLuceneConverter) extends MultiValueConverterBase(elementConverter){
  override def appendDocValues(value: Any, name: String, doc: Document): Unit = {
    val docValue=new SortedNumericDocValuesField(name, elementConverter.asInstanceOf[NumericBasedConverter].valueToLong(value))
    doc.add(docValue)
  }
}

class ComplexMultiValueConverter(elementConverter:RowToLuceneConverter) extends MultiValueConverterBase(elementConverter){
  override def appendDocValues(value: Any, name: String, doc: Document): Unit = {
  }
}


/**
 * @param keyConverter
 * @param valueConverter
 */
class MapConverter(keyConverter:RowToLuceneConverter,valueConverter:RowToLuceneConverter) extends RowToLuceneConverter {
  val multiValueConverter=MultiValueConverter(keyConverter)
  override def append(row: SpecializedGetters, ordinal: Int, name: String, doc: Document): Unit = {
    val map=row.getMap(ordinal)
    val length =map.numElements()
    val keys =map.keyArray()
    val values = map.valueArray()
    appendSize(length,name,doc)
    for(i<- 0 until length){
      val key= keyConverter.getValue(keys,i)
      val fullKey=Array(name,key.toString).quoted
      appendFieldName(fullKey,doc)
      multiValueConverter.append(keys,i,name, doc)
      if(!values.isNullAt(i))
      {
          valueConverter.append(values,i,fullKey,doc)
      }
    }
  }

  override def appendIndex(value: Any, name: String, doc: Document): Unit = {}

  override def appendDocValues(value: Any, name: String, doc: Document): Unit = {
    val map=value.asInstanceOf[MapData]
    val length =map.numElements()
    val keys =map.keyArray()
    val values = map.valueArray()
    appendSize(length,name,doc)
    for(i<- 0 until length){
      val key= keyConverter.getValue(keys,i)
      val fullKey=Array(name,key.toString).quoted
      appendFieldName(fullKey,doc)
      multiValueConverter.appendDocValues(key,name,doc)
      if(!values.isNullAt(i))
      {
        val value=valueConverter.getValue(values,i)
        valueConverter.appendDocValues(value,fullKey,doc)
      }
    }
  }

  override def getValue(row: SpecializedGetters, ordinal: Int): Any = {
    row.getMap(ordinal)
  }
}

class StructConverter(structType:StructType,childConverters:Array[RowToLuceneConverter]) extends RowToLuceneConverter {
  override def append(row: SpecializedGetters, ordinal: Int, name: String, doc: Document): Unit = {
     if(name!=null && name.nonEmpty){
       appendFieldNameDocValue(name,doc)
     }
    val struct=row.getStruct(ordinal,childConverters.length)
    for(i<- 0 until childConverters.length) {
       if(!struct.isNullAt(i)){
         val childName = Array(name, structType.fields(i).name).quoted
         appendFieldName(childName,doc)
         childConverters(i).append(struct, i, childName, doc)
       }
     }
  }

  override def appendIndex(value: Any, name: String, doc: Document): Unit = {}

  override def appendDocValues(value: Any, name: String, doc: Document): Unit = {
    val struct=value.asInstanceOf[InternalRow]
    if(name!=null && name.nonEmpty){
      appendFieldNameDocValue(name,doc)
    }
    for(i<- 0 until childConverters.length) {
      if(!struct.isNullAt(i)){
        val childName = Array(name, structType.fields(i).name).quoted
        childConverters(i).appendDocValues(getValue(struct,i), childName, doc)
      }
    }
  }
  override def getValue(row: SpecializedGetters, ordinal: Int): Any = {
     row.getStruct(ordinal,childConverters.length)
  }
}

class ArrayConverter(elementConverter:RowToLuceneConverter) extends RowToLuceneConverter{
  val multiValueConverter=MultiValueConverter(elementConverter)
  override def append(row: SpecializedGetters, ordinal: Int, name: String, doc: Document): Unit = {
    val array=row.getArray(ordinal)
    appendSize(array.numElements(),name,doc)
    var i = 0
    while (i < array.numElements()) {
      if (!array.isNullAt(i)) {
        val value=elementConverter.getValue(array,i)
        // Array<Struct ,Map,Array 均不需要index，因为无法下推>
        elementConverter.appendIndex(value,name,doc)
        elementConverter.appendDocValues(value, s"$name[$i]", doc)
      }
      i += 1
    }
  }

  override def appendIndex(value: Any, name: String, doc: Document): Unit = {
  }

  override def appendDocValues(value: Any, name: String, doc: Document): Unit = {
    val array=value.asInstanceOf[ArrayData]
    appendSize(array.numElements(),name,doc)
    var i = 0
    while (i < array.numElements()) {
      if (!array.isNullAt(i)) {
        val value=elementConverter.getValue(array,i)
        elementConverter.appendDocValues(value, s"$name[$i]", doc)
      }
      i += 1
    }
  }

  override def getValue(row: SpecializedGetters, ordinal: Int): Any = {
    row.getArray(ordinal)
  }
}