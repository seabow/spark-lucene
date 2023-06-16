package io.github.seabow.lucene.benchmark
import io.github.seabow.lucene.PerformanceUtil
import org.scalameter.api._

object ReadBenchMark extends Bench.OfflineRegressionReport {
  val numRecords = Gen.range("numRecords")(300000, 600000, 300000)

  private val readOpts = Context(
    exec.minWarmupRuns -> 1,
    exec.maxWarmupRuns -> 1,
    exec.benchRuns -> 3,
    exec.independentSamples -> 1,
    exec.jvmflags -> List("-Xms4g", "-Xmx4g")
  )

  private val beforeWriting={
    numRecord =>
      generateRandom(numRecord) // 在基准测试之前先生成随机文件
  }

  // 准备阶段
  def generateRandom(numRecords: Int): Unit = {
    println(s"generating random $numRecords")
    // 在这里编写生成随机文件的逻辑
    PerformanceUtil.generateRandom(numRecords)
  }
  val condition="array_contains(map_tags.`sports`,'basketball')"

  performance of "Reading test" config readOpts  in {
    measure method "read orc" in {
      using(numRecords) setUp beforeWriting  in {
        num => PerformanceUtil.read(num, "orc").filter(condition).count() // 执行基准测试
      }
    }
    measure method "read lucene" in {
      using(numRecords) setUp beforeWriting in {
        num => PerformanceUtil.read(num, "lucene").filter(condition).count() // 执行基准测试
      }
    }
  }
}
