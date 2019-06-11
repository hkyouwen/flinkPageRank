package flink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object PageRankData {

  val EDGES: Array[(Long,Long)] = Array(
    (1L, 2L),
    (1L, 15L),
    (2L, 3L),
    (2L, 4L),
    (2L, 5L),
    (2L, 6L),
    (2L, 7L),
    (3L, 13L),
    (4L, 2L),
    (5L, 11L),
    (5L, 12L),
    (6L, 1L),
    (6L, 7L),
    (6L, 8L),
    (7L, 1L),
    (7L, 8L),
    (8L, 1L),
    (8L, 9L),
    (8L, 10L),
    (9L, 14L),
    (9L, 1L),
    (10L, 1L),
    (10L, 13L),
    (11L, 12L),
    (11L, 1L),
    (12L, 1L),
    (13L, 14L),
    (14L, 12L),
    (15L, 1L))

  val numPages = 15

  def getDefaultEdgeDataSet(env: ExecutionEnvironment): DataSet[(Long, Long)] = {
    env.fromCollection(EDGES)
  }

  def getDefaultPagesDataSet(env: ExecutionEnvironment): DataSet[Long] = env.generateSequence(1, 15)
}
