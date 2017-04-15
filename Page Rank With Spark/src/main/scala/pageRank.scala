/**
  * Created by amogh-hadoop on 3/8/17.
  */

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;




object pageRank {
  def main(args: Array[String]) {
    val alpha: Double = 0.15
    val alpha_rem: Double = 1 - alpha
    val conf = new SparkConf().
      setAppName("pageRank").
      setMaster("local[*]")
      //setMaster("yarn")

    val sc = new SparkContext(conf)


    // reading path of input file through command line
    val data = sc.textFile(args(0)+"/*.bz2").
      map(line => WikiParser.makeGraph(line)).
      filter(graphData => graphData != null).
      map(nodeName => nodeName.split("######")). //splitting based on a sequence of text
      map(name2 => (name2(0),
      if (name2.size > 1) name2(1).split(", ").toList else List[String]())).
      map(node => List((node._1, node._2)) ++ node._2.
        map(adjNode => (adjNode, List[String]()))). // Create missing dangling nodes
      flatMap(node => node).
      reduceByKey((x, y) => (x ++ y)).
      persist()


    //getting value of |N|
    val pageCount = data.count()


    //assigning default page rank to each node . i.e 1/|N|
    val ranks = data.map(node => (node._1, 1.0 / pageCount))


    var graph = data.join(ranks);

    for (i <- 1 to 10) {

      //logic to compute delta
      val delta = graph.filter(node => node._2._1.length == 0).
        reduce((a, b) => (a._1, (a._2._1, a._2._2 + b._2._2)))._2._2

      // adding page rank contribution to each node
      val page_ranks = graph.values
        .map(adjNodes => (adjNodes._1.map(node => (node, adjNodes._2 / adjNodes._1.size)))).
        flatMap(node => node).
        reduceByKey((x, y) => x + y)

      //adding delta factors to the nodes using left join
      graph = data.leftOuterJoin(page_ranks).
        map(n1 => {
          (n1._1, (n1._2._1, n1._2._2 match {
            case None => (alpha / pageCount) + ((alpha_rem) * delta / pageCount)
            case Some(x: Double) => (alpha / pageCount) + (alpha_rem * ((delta / pageCount) + x))
          }))
        })
    }


    val output = graph.map(v => {
      (v._2._2, v._1)
    }).
      top(100)

    //the output path is supplied via command line
    sc.parallelize(output, 1).saveAsTextFile(args(1))

  }
}
