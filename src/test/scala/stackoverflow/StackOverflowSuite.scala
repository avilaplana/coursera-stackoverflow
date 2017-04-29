package stackoverflow

import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with Matchers with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }


  test("groupedPostings should group questions and answers grouped together") {
    val posts = StackOverflow.sc.parallelize(Seq(
      Posting(1, 1, None, None, 10, None),
      Posting(1, 2, None, None, 10, None),
      Posting(2, 3, None, Some(1), 10, None),
      Posting(2, 4, None, Some(1), 9, None),
      Posting(2, 5, None, Some(1), 8, None),
      Posting(2, 6, None, Some(2), 10, None),
      Posting(2, 7, None, Some(2), 10, None)
    ))

    val grouped: Array[(Int, Iterable[(Posting, Posting)])] = StackOverflow.groupedPostings(posts).collect()

    grouped.map(e => (e._1, e._2.toList)) should be(Array( // to fix this
      (1, Iterable(
        (Posting(1, 1, None, None, 10, None), Posting(2, 3, None, Some(1), 10, None)),
        (Posting(1, 1, None, None, 10, None), Posting(2, 4, None, Some(1), 9, None)),
        (Posting(1, 1, None, None, 10, None), Posting(2, 5, None, Some(1), 8, None))
      )),
      (2, Iterable(
        (Posting(1, 2, None, None, 10, None), Posting(2, 6, None, Some(2), 10, None)),
        (Posting(1, 2, None, None, 10, None), Posting(2, 7, None, Some(2), 10, None))
      ))
    ))
  }
}
