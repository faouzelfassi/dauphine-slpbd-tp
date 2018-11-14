/* A solution to universal sinks problem */
// Example 1:
val array = Array((1,4), (2,4), (3,4), (3,2))
val g1 = sc.parallelize(array)
val nodes = g1.flatMap(x => Array(x._1, x._2))
val indegree=g1.map(x => (x._2, 1)).reduceByKey((x,y) => x+y)
val n = nodes.distinct.count
val sinks = indegree.filter(x => x._2 == n-1).map(_._1).collect

// Example 2:
val array = Array((1,2), (1,5), (1,6), (2,5), (2,6), (3,5), (3,6), (4,2), (4,5), (4,6))
val g1 = sc.parallelize(array)
val nodes = g1.flatMap(x => Array(x._1, x._2))
val indegree=g1.map(x => (x._2, 1)).reduceByKey((x,y) => x+y)
val n = nodes.distinct.count
val sinks = indegree.filter(x => x._2 == n-1).map(_._1).collect


/* A solution for max indegrees sink problem */
val array = Array((1,2), (1,5), (1,6), (2,5), (2,6), (3,5), (3,6), (4,2), (4,5), (4,6))
val g1 = sc.parallelize(array)
val nodes = g1.flatMap(x => Array(x._1, x._2))
val indegree=g1.map(x => (x._2, 1)).reduceByKey((x,y) => x+y).persist
val n = indegree.max()(new Ordering[Tuple2[Int, Int]]() {
  override def compare(x: (Int, Int), y: (Int, Int)): Int =
      Ordering[Int].compare(x._2, y._2)
})._2
val outdegree=g1.map(x => (x._1, 1))
val sinks = indegree.filter(x => x._2 == n).leftOuterJoin(outdegree).map(_._1).collect()


// With Input file
val g = sc.textFile("file:///Users/faouz/Documents/Cours/TP_DAUPHINE/TP2/graph.txt")
val g1 = g.map(x => ( x.split("\t")(0), x.split("\t")(2)))
val nodes = g1.flatMap(x => Array(x._1, x._2))
val indegree=g1.map(x => (x._2, 1)).reduceByKey((x,y) => x+y).persist
val n = indegree.max()(new Ordering[Tuple2[String, Int]]() {
  override def compare(x: (String, Int), y: (String, Int)): Int =
      Ordering[Int].compare(x._2, y._2)
})._2
val outdegree = g1.map(x => (x._1, 1))
val sinks = indegree.filter(x => x._2 == n).leftOuterJoin(outdegree).map(_._1).distinct.collect()
