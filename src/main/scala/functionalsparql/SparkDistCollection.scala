package eu.liderproject.functionalsparql

import scala.collection.TraversableOnce
import scala.reflect.ClassTag
import org.apache.spark.rdd.{PairRDDFunctions, RDD}

case class SparkDistCollection[A](rdd : RDD[A])(implicit vt : ClassTag[A]) extends DistCollection[A] {
  override def map[B](foo : A => B)(implicit tag : ClassTag[B]) = SparkDistCollection(rdd.map(foo))
  override def flatMap[B](foo : A => TraversableOnce[B])(implicit tag : ClassTag[B]) = SparkDistCollection(rdd.flatMap(foo))
  override def filter(foo : A => Boolean) = SparkDistCollection(rdd.filter(foo))
  override def count() = rdd.count()
  override def key[K](foo : A => K)(implicit ordering : math.Ordering[K], kt : ClassTag[K]	) = SparkKeyDistCollection(rdd.map(a => (foo(a),a)))
  override def keyFilter[K](foo : A => Option[K])(implicit ordering : math.Ordering[K], kt : ClassTag[K]) = SparkKeyDistCollection(rdd.flatMap(a => foo(a).map((_,a))))
  override def cartesian[B](other : DistCollection[B])(implicit tag : ClassTag[B]) = other match {
  	case SparkDistCollection(otherRDD) => SparkDistCollection(rdd.cartesian(otherRDD))
  	case _ => throw new IllegalArgumentException("Cartesian product of RDD with non-RDD is not allowed")
  }
  override def toIterable = new Iterable[A] {
  	def iterator = rdd.toLocalIterator
  }
  override def exists(foo : A => Boolean) = {
    // TODO: There must be a better way
    rdd.count() > 0
  }
  override def ++(other : DistCollection[A]) = other match {
  	case SparkDistCollection(otherRDD) => SparkDistCollection(rdd ++ otherRDD)
	case _ => throw new IllegalArgumentException("Cartesian product of RDD with non-RDD is not allowed")
  }
  override def unique = throw new UnsupportedOperationException("TODO")
  override def drop(n : Long) = throw new UnsupportedOperationException("TODO")
  override def take(n : Long) = throw new UnsupportedOperationException("TODO")
}

case class SparkKeyDistCollection[K, A](rdd : RDD[(K, A)])(implicit kt : ClassTag[K], vt : ClassTag[A], ord : math.Ordering[K]) extends KeyDistCollection[K, A] {
  def join[B](to : KeyDistCollection[K,B]) = to match {
  	case SparkKeyDistCollection(otherRDD) => SparkDistCollection(new PairRDDFunctions(rdd).join(otherRDD).map(_._2))
  	case _ => throw new IllegalArgumentException("Join of RDD with non-RDD is not allowed")
  }
  def cogroup[B](to : KeyDistCollection[K,B]) = to match {
  	case SparkKeyDistCollection(otherRDD) => SparkDistCollection(new PairRDDFunctions(rdd).cogroup(otherRDD).map(_._2))
  	case _ => throw new IllegalArgumentException("Cogroup of RDD with non-RDD is not allowed")
  }
  def toIterable = new Iterable[(K, A)] {
  	def iterator = rdd.toLocalIterator
  }
  def ++(other : KeyDistCollection[K,A]) = other match {
  	case SparkKeyDistCollection(otherRDD) => SparkKeyDistCollection(rdd ++ otherRDD)
	case _ => throw new IllegalArgumentException("Cartesian product of RDD with non-RDD is not allowed")
  }
}