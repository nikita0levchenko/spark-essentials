package playground

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

object ScalaSyntaxPlayground extends App {

  import scala.concurrent.ExecutionContext.Implicits.global

  val future: Future[Int] = Future {42}
  Thread.sleep(1000)
  val valid = for {
    res <- future
    if res < 0
  } yield res
  Thread.sleep(1000)
  println(s"is complete: ${valid.isCompleted} | value: ${valid.value}")

  val collected: Future[Int] = future.collect {
    case res if res < 50 => res * 3
  }
  Thread.sleep(1000)
  println(s"is complete: ${collected.isCompleted} | value: ${collected.value}")

  val collected1: Future[Int] = collected.collect {
    case res if res < 50 => res * 3
    case res: Int => res / 2
  }
  Thread.sleep(1000)
  println(s"is complete: ${collected1.isCompleted} | value: ${collected1.value}")


}
