package eu.liderproject.functionalsparql

import scala.collection.TraversableOnce
import scala.reflect.ClassTag

trait DistCollection[A]  {
  def map[B](foo : A => B)(implicit tag : ClassTag[B]) : DistCollection[B]
  def flatMap[B](foo : A => TraversableOnce[B])(implicit tag : ClassTag[B]) : DistCollection[B]
  def filter(foo : A => Boolean) : DistCollection[A]
  def count() : Long
  def exists(foo : A => Boolean) : Boolean
  def ++(other  : DistCollection[A]) : DistCollection[A]
  def unique : DistCollection[A]
  def drop(n : Long) : DistCollection[A]
  def take(n : Long) : DistCollection[A]
  def cartesian[B](other : DistCollection[B])(implicit tag : ClassTag[B]) : DistCollection[(A,B)]

  def key[K](foo : A => K)(implicit ordering : math.Ordering[K], kt : ClassTag[K]) : KeyDistCollection[K,A]
  def keyFilter[K](foo : A => Option[K])(implicit ordering : math.Ordering[K], kt : ClassTag[K]) : KeyDistCollection[K, A]
  def toIterable : Iterable[A]
}

trait KeyDistCollection[K,A] {
  def join[B](to : KeyDistCollection[K,B]) : DistCollection[(A,B)]
  def leftJoin[B](to : KeyDistCollection[K,B]) : DistCollection[(A, Option[B])]
  def toIterable : Iterable[(K, A)]
  def ++(other : KeyDistCollection[K,A]) : KeyDistCollection[K, A]
  def sorted : DistCollection[A]
}

case class SimpleDistCollection[A](seq : Seq[A]) extends DistCollection[A] {
  override def map[B](foo : A => B)(implicit tag : ClassTag[B]) = SimpleDistCollection(seq.map(foo))
  override def flatMap[B](foo : A => TraversableOnce[B])(implicit tag : ClassTag[B]) = SimpleDistCollection(seq.flatMap(foo))
  override def filter(foo : A => Boolean) = SimpleDistCollection(seq.filter(foo))
  override def count() = seq.size
  override def key[K](foo : A => K)(implicit ordering : math.Ordering[K], kt : ClassTag[K]) = SimpleKeyDistCollection(seq.map(a => (foo(a),a)))
  override def keyFilter[K](foo : A => Option[K])(implicit ordering : math.Ordering[K], kt : ClassTag[K]) = SimpleKeyDistCollection(seq.flatMap(a => foo(a).map((_,a))))
  override def cartesian[B](other : DistCollection[B])(implicit tag : ClassTag[B]) = SimpleDistCollection(seq.flatMap { a => 
    other.toIterable.map { b => 
      (a,b)
    }
  })
  override def exists(foo : A => Boolean) = seq.exists(foo)
  override def toIterable = seq
  override def ++(other : DistCollection[A]) = SimpleDistCollection(seq ++ other.toIterable)
  override def unique = SimpleDistCollection(seq.toSet.toSeq)
    
  def drop(n : Long) = SimpleDistCollection(seq.drop(n.toInt))
  def take(n : Long) = SimpleDistCollection(seq.take(n.toInt))
}

case class SimpleKeyDistCollection[K, A](seq : Seq[(K, A)])(implicit ordering : math.Ordering[K]) extends KeyDistCollection[K, A] {
  def join[B](to : KeyDistCollection[K,B]) = this.cogroup(to).flatMap {
    case (xs,ys) => xs.flatMap { x =>
      ys.map { y =>
        (x,y)
      }
    }
  }
  def leftJoin[B](to : KeyDistCollection[K, B]) = this.cogroup(to).flatMap {
    case (xs, ys) => if(ys.isEmpty) {
      xs.map((_, None))
    } else {
      xs.flatMap { x =>
        ys.map { y =>
          (x, Some(y))
        }
      }
    }
  }
  def cogroup[B](to : KeyDistCollection[K,B]) = to match {
    case SimpleKeyDistCollection(seq2) => {

      val sort1 = seq.sortBy(_._1)(ordering)
      val sort2 = seq2.sortBy(_._1)(ordering)
      val iter1 = new PeekableIterator(sort1.iterator)
      val iter2 = new PeekableIterator(sort2.iterator)

      def compareNext[Z](iter : PeekableIterator[(K,Z)], k : K) = {
        iter.peek match {
          case Some((z,_)) => ordering.compare(z,k) == 0
          case None => false
        }
      }

      def doStream() : Stream[(Iterable[A],Iterable[B])]= if(iter1.hasNext || iter2.hasNext) {
          val o = if(iter1.hasNext && iter2.hasNext) {
            ordering.compare(iter1.peek.get._1,iter2.peek.get._1)
          } else if(iter1.hasNext) {
            -1
          } else {
            +1
          }
          val r = if(o == 0) {
            val (k1,v1) = iter1.next
            val (k2,v2) = iter2.next
            var v1s = List(v1)
            var v2s = List(v2)
            while(compareNext(iter1,k1)) {
              v1s ::= iter1.next._2
            }
            while(compareNext(iter2,k2)) {
              v2s ::= iter2.next._2
            }
            (v1s,v2s)
          } else if(o < 0) {
            val (k1,v1) = iter1.next
            var v1s = List(v1)
            while(compareNext(iter1,k1)) {
              v1s ::= iter1.next._2
            }
            (v1s,Nil)
          } else {
            val (k2,v2) = iter2.next
            var v2s = List(v2)
            while(compareNext(iter2,k2)) {
              v2s ::= iter2.next._2
            }
            (Nil,v2s)
          }
          r #:: doStream()
        } else {
          Stream()
        }
      SimpleDistCollection(doStream())

    }
  }
  override def toIterable = seq
  override def ++(other : KeyDistCollection[K, A]) = SimpleKeyDistCollection(seq ++ other.toIterable)
  override def sorted = SimpleDistCollection(seq.sortBy(_._1)(ordering).map(_._2))
}

class PeekableIterator[A](base : Iterator[A]) extends Iterator[A] {
  private var last : Option[A] = None

  override def next = last match {
    case Some(a) => {
      last = None
      a
    }
    case None => base.next
  }

  override def hasNext = last != None || base.hasNext

  def peek : Option[A] = {
    last match {
      case Some(_) => 
      case None => {
        if(base.hasNext) {
          last = Some(base.next)
        }
      }
    }
    last
  }
}

object DistCollection {
  def prettyPrint(dc : DistCollection[_], indent : Int) :String = {
    "  " * indent + "DistCollection(\n" + (dc.toIterable.map { elem => elem match {
      case dc2 : DistCollection[_] => "  " * (indent + 1) + prettyPrint(dc2, indent + 1)
      case dc2 : KeyDistCollection[_,_] => "  " * (indent + 1) + prettyPrint(dc2, indent + 1)
      case other => "  " * (indent + 1) + other.toString
    }
  }).mkString("\n") + ")"
  }

  def prettyPrint(dc : KeyDistCollection[_,_], indent : Int) :String = {
    "  " * indent + "DistKeyCollection(\n" + (dc.toIterable.map { elem => elem match {
      case (k, dc2 : DistCollection[_]) => "  " * (indent + 1) + k.toString + " -> " + prettyPrint(dc2, indent + 1)
      case (k, dc2 : KeyDistCollection[_,_]) => "  " * (indent + 1) + k.toString + " -> " + prettyPrint(dc2, indent + 1)
      case (k, other) => "  " * (indent + 1) + k.toString + " -> " + other.toString
    }
  }).mkString("\n") + ")"
  }
}


