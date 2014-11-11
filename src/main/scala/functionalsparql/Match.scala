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
    l == r || (l.isBlank() && (r.isBlank() || !r.isLiteral())) || (r.isBlank() && (!l.isLiteral()))
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

