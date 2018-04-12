package map_reduce

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.RecursiveTask
import java.util.concurrent.ForkJoinWorkerThread

import scala.collection.mutable.{MutableList}
import scala.concurrent.{Await, Future}
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object runnable {

  def mergeMap[A, B](ms: List[Map[A, B]])(f: (B, B) => B): Map[A, B] =
    (Map[A, B]() /: (for (m <- ms; kv <- m) yield kv)) { (a, kv) =>
      a + (if (a.contains(kv._1)) kv._1 -> f(a(kv._1), kv._2) else kv)
    }

  def mapReduce[T](items: Vector[Int], workers: Int): Map[Int, Int] = {
    val unsorted = items
    val blockSize = (items.length / workers.toFloat).ceil.toInt
    val splited = unsorted.grouped(blockSize).toList
    val keys = scala.collection.mutable.SortedSet[Int]()

    val mapFutures = Future.traverse(splited) { items =>
      Future {
        items.groupBy(w => w).mapValues(_.size)
      }
    }

    val maps = Await.result(mapFutures, Duration.Inf)
    val workersIds = List.range(0, workers)

    val shuffleFutures = Future.traverse(workersIds) { workerId =>
      Future {
        maps.map(map => map.filter(element => workerId == (element._1.hashCode() % workers))).filter(!_.isEmpty)
      }
    }

    val shuffleList = Await.result(shuffleFutures, Duration.Inf)

    val reduceFutures = Future.traverse(shuffleList) { items =>
      Future {
        mergeMap(items)((v1, v2) => v1 + v2)
      }
    }

    val reduces = Await.result(reduceFutures, Duration.Inf)
    return reduces.flatten.toMap
  }

  def main(args: Array[String]): Unit = {
    val elementsСount = 10000000
    val items = Vector.fill(elementsСount)(Random.nextInt(10))

    val t0 = System.currentTimeMillis()
    val map = mapReduce(items, 4)
    val t1 = System.currentTimeMillis()
    print(map)
    print("\n", map.foldLeft(0)(_+_._2) == elementsСount)
    println("\nElapsed time: " + (t1 - t0) + "ms")
  }

}
