package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  class Animal

  class Dog extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("crunch")
  }

  object Carnivore {
    def growl(): Unit = println("growwwwwl")
  }

  val increment: Int => Int = (x: Int) => x + 1

  val unknown: Any = 45
  val ordinal = unknown match {
    case 45 => "HH"
    case _ => "really unknown"
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    Thread.sleep(1000)
    42
  }

  aFuture.onComplete {
    case Success(age) => println(age)
    case Failure(exception) => println(s"sorry, $exception is up")
  }

  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case _ => 1000
  }

  def autoImplicit(implicit x: Int): Int = x + 43

  implicit val number: Int = 1

  case class Person(name: String) {
    def greet: String = s"hello my name is $name"
  }

  implicit def fromStringToPerson(name: String): Person = Person(name)
//  println("john".greet)

  implicit class Cat(name:String) {
    def mow:Unit = println("mow")
  }

//  "cat".mow
}
