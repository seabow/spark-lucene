package io.github.seabow.spark.v2.lucene.collector

import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.DocIdSet
import org.apache.lucene.util.DocIdSetBuilder

import scala.collection.mutable.ListBuffer

class PagingLeafReaderStoreCollector(page: Int, resultsPerPage: Int) extends PagingBasedCollector(page, resultsPerPage) {
  private var context: LeafReaderContext = null
  private var size: Int = 0
  private val leafReaderStores: ListBuffer[LeafReaderStore] = ListBuffer.empty[LeafReaderStore]
  private var docsBuilder: DocIdSetBuilder = _

  //最后一个leafreader在collect完成后无法调用doSetNextReader中的 append leafReaderStore，因此在get时加上。
  def getLeafReaderStores: ListBuffer[LeafReaderStore] = {
    if (size>0) {
      leafReaderStores.append(LeafReaderStore(this.context, docsBuilder.build, size))
      size=0
    }
    leafReaderStores
  }

  override def doSetNextReader(context: LeafReaderContext): Unit = {
    if (docsBuilder != null){
      leafReaderStores.append(new LeafReaderStore(this.context, docsBuilder.build, size))
    }
    docsBuilder = new DocIdSetBuilder(context.reader.maxDoc)
    size = 0
    this.context = context
  }
  override def collectOne(docId: Int): Unit = {
    docsBuilder.grow(1).add(docId)
    size += 1
  }
}

case class LeafReaderStore(context: LeafReaderContext, docIdSet: DocIdSet, size: Int)
