package io.github.seabow.lucene

import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.document.{BinaryDocValuesField, Document, NumericDocValuesField}
import org.apache.lucene.index._
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery}
import org.apache.lucene.store.RAMDirectory
import org.apache.lucene.util.BytesRef
import org.apache.spark.unsafe.types.UTF8String

object DocValuesExample {
  def main(args: Array[String]): Unit = {
    // 创建内存存储
    val directory = new RAMDirectory()

    // 创建索引写入器
    val config = new IndexWriterConfig(new WhitespaceAnalyzer())
    val writer = new IndexWriter(directory, config)

    // 创建自定义无序多值的DocValues字段
    val field1 = new BinaryDocValuesField("myField1",new BytesRef("Phạm Uyển Trinh"))
    val field2 = new NumericDocValuesField("myField2", 2)
    val field3 = new NumericDocValuesField("myField3", 3)
    val field4 = new NumericDocValuesField("myField4", 4)

    // 创建文档并添加字段
    val document = new Document()
    document.add(field1)
    document.add(field2)
    document.add(field3)
    document.add(field4)

    // 将文档写入索引
    writer.addDocument(document)
    writer.commit()
    writer.close()

    // 创建索引搜索器
    val reader = DirectoryReader.open(directory)
    val searcher = new IndexSearcher(reader)

    // 执行查询
    val topDocs = searcher.search(new MatchAllDocsQuery,1)
    // 遍历结果并输出顺序
    // 关闭读取器和存储
   val docValues= MultiDocValues.getBinaryValues(reader,"myField1")
    topDocs.scoreDocs.foreach{
      doc=>
        if(docValues.advanceExact(doc.doc)){
          println(UTF8String.fromBytes(docValues.binaryValue().bytes))
        }
    }
    directory.close()
  }
}
