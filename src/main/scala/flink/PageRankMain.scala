package flink

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object PageRankMain {
  val DAMPENING_FACTOR:Double = 0.85
  val EPSILON:Double = 0.1
  def main(args: Array[String]): Unit = {
    val numPages = PageRankData.numPages
    val maxIterations = 10

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    // get input data
    val pagesInput:DataSet[Long] =  PageRankData.getDefaultPagesDataSet(env)
    val linksInput:DataSet[(Long, Long)] = PageRankData.getDefaultEdgeDataSet(env)

    // 初始化rank <pageID,rank>,初始rank为1分
    val pagesWithRanks:DataSet[(Long,Double)] = pagesInput.map{(_,1.0d)}

    // 获取<pageID,NeighborsNum,Neighbor>
    val adjacencyListInput:DataSet[(Long,Int, Long)] = linksInput
      .map{i=>(i._1, 1)} //<startPage,1>
      .groupBy(0)
      .sum(1)
      .join(linksInput)
      .where(0)
      .equalTo(0)
      .map{i=>(i._1._1,i._1._2,i._2._2)}

    // set iterative data set
    val iteration = pagesWithRanks.iterateWithTermination(maxIterations) {
      iterationSet =>
        val newRanks: DataSet[(Long, Double)] = iterationSet
          .join(adjacencyListInput)
          .where(0)
          .equalTo(0)
          .map { i => (i._2._3, i._1._2 / i._2._2) } //<Neighbor,startPageRank/NeighborsNum>
          .groupBy(0)
          .sum(1)
          .map { i => (i._1, i._2 * DAMPENING_FACTOR + (1 - DAMPENING_FACTOR)) } //<Neighbor,(1-d)+d*NeighborRank>
          //.map { i => (i._1, i._2 * DAMPENING_FACTOR + (1 - DAMPENING_FACTOR)/numPages) } // other function <Neighbor,(1-d)/numPages+d*NeighborRank>
        val term = newRanks
          .join(iterationSet)
          .where(0)
          .equalTo(0)
          .filter { i => Math.abs(i._1._2 - i._2._2) > EPSILON } // 终止条件，上次结果与本次结果之差小与EPSILON
        (newRanks, term)
    }
    // emit result
  iteration.print()
  }
}

