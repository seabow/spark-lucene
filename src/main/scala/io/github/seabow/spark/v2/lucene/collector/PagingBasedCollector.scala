package io.github.seabow.spark.v2.lucene.collector

import org.apache.lucene.search.{ScoreMode, SimpleCollector}

abstract class PagingBasedCollector(page: Int, resultsPerPage: Int) extends SimpleCollector{
  protected var currentPage: Int = page
  protected var collectedResults: Int = 0
  protected var hasNextPage=false

  override def scoreMode(): ScoreMode = ScoreMode.COMPLETE_NO_SCORES
  override def collect(doc: Int): Unit = {
    // 检查是否已经收集足够的结果，如果是则停止收集
    if (collectedResults >= currentPage * resultsPerPage) {
      hasNextPage=true
      return
    }

    // 检查是否达到当前页的起始位置，如果是则开始收集结果
    if (collectedResults >= (currentPage - 1) * resultsPerPage) {
      // 处理结果
      collectOne(doc)
    }

    collectedResults += 1
  }

  def collectOne(docId:Int):Unit

}
