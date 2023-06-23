package io.github.seabow.spark.v2.lucene.collector

import org.apache.lucene.index.LeafReaderContext

import scala.collection.mutable.ListBuffer

class PagingDocIdsCollector(page: Int, resultsPerPage: Int) extends PagingBasedCollector(page, resultsPerPage) {
  var docs=ListBuffer[Int]()
  private var docBase: Int = 0

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    docBase = context.docBase
  }

  override def collectOne(docId: Int): Unit = {
    docs.append(docBase+docId)
  }
}
