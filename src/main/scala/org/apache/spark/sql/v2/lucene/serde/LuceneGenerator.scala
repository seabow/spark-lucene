package org.apache.spark.sql.v2.lucene.serde

import io.github.seabow.HdfsDirectory
import io.github.seabow.spark.v2.lucene.LuceneOptions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.codecs.lucene87.Lucene87Codec
import org.apache.lucene.codecs.lucene87.Lucene87Codec.Mode
import org.apache.lucene.document._
import org.apache.lucene.index.IndexWriterConfig.OpenMode
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

class LuceneGenerator(val path: String, val dataSchema: StructType, val conf: Configuration, val options: LuceneOptions) {
  val dirPath=s"$path.dir"
  val (dir, writer) = {
    val dir = new HdfsDirectory(new Path(dirPath), new Configuration)
    // 分析器
    val analyzer = new StandardAnalyzer
    // 索引配置
    val iwc = new IndexWriterConfig(analyzer)
    iwc.setOpenMode(OpenMode.CREATE_OR_APPEND)
    iwc.setCodec(new Lucene87Codec(Mode.BEST_SPEED))
    iwc.setUseCompoundFile(true)
    iwc.setRAMBufferSizeMB(128)
    iwc.setMaxBufferedDocs(1024*1024)
    // 索引写入器
    val writer = new IndexWriter(dir, iwc)
    (dir, writer)
  }


  def writeSchema():Unit={
    val hdfs=dir.getFileSystem
    val schema=new Path(dirPath+"/.schema")
    if(!hdfs.exists(schema)){
      val dos=hdfs.create(schema)
      val bytes=dataSchema.toDDL.getBytes()
      dos.write(bytes, 0, bytes.length)
      dos.close()
    }
    val partitionFilePath=new Path(path)
    if(!hdfs.exists(partitionFilePath)){
      val dos=hdfs.create(partitionFilePath)
      val bytes=dirPath.getBytes()
      dos.write(bytes, 0, bytes.length)
      dos.close()
    }
  }

  private val converters:Seq[RowToLuceneConverter]=dataSchema.map(_.dataType).map(RowToLuceneConverter.makeConverter(_))

  def write(row: InternalRow): Unit = {
    val doc = new Document
    //TODO 使用docValues代替StoredField。
//    doc.add(new StoredField("_source",new BytesRef(storeFieldAvroWriter.getAndReset(row))))
    for (idx <- 0 until row.numFields) {
      if (!row.isNullAt(idx)) {
        converters(idx).append(row,idx,dataSchema(idx).name,doc)
        doc.add(new StringField( "_field_names",dataSchema(idx).name, Field.Store.NO))
      }
    }
    writer.addDocument(doc)
  }

  def close(): Unit = {
    writer.flush()
    writer.commit()
//    writer.close()
//    dir.close()
  }
}
