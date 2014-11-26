package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.sparql.core.Quad
import java.io.File
import scala.xml.XML

//sealed trait Match {
//  def &(m : Match) : Match
//}

case class Match(triples : Set[Quad], binding : Map[String, Node]) {
  def compatible(m : Match) = (binding.keys.toSet & m.binding.keys.toSet) forall { k =>
    val l = binding(k)
    val r = m.binding(k)
    l == r || (l.isBlank() && r.isURI()) || (r.isBlank() && r.isURI())
  }

  def &(m : Match) = {
  	require(compatible(m))
        val newBindings = binding ++ (m.binding.map { case (k,v) =>
          if(v.isBlank) {
            k -> binding.getOrElse(k, v)
          } else {
            k -> v
          }
        })
  	Match(triples ++ m.triples, newBindings)
  }
}

/*object Match {
  private def compareKeys(k1 : Iterator[String], k2 : Iterator[String]) : Int = {
    while(k1.hasNext && k2.hasNext) {
      val v1 = k1.next
      val v2 = k2.next
      if(v1 < v2) {
        return -1
      } else if(v1 > v2) {
        return +1
      }
    }
    if(k1.hasNext) {
      return -1
    } else if(k2.hasNext) {
      return +1
    } else {
      return 0
    }
  }

  implicit val ordering = new Ordering[Match] {
    def compare(m1 : Match, m2 : Match) : Int = {
      if(m1.binging.size < m2.binding.size || m1.triples.size < m2.triples.size) {
        return -1
      } else if(m1.binding.size > m2.binding.size || m1.triples.size > m2.bindings.size) {
        return +1
      } else {

      }
    }
  }
}*/

