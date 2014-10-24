package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.{Node, Triple}
import java.io.File
import scala.xml.XML

sealed trait Match {
  def &(m : Match) : Match
}

case class TripleMatch(triples : Seq[Triple], binding : Map[String, Node]) extends Match {
  def &(m : Match) = m match {
    case t : TripleMatch => 
      val compatible = (binding.keys.toSet & t.binding.keys.toSet) forall { k =>
        binding(k) == t.binding(k)
      }
      if(compatible) {
        TripleMatch(triples ++ t.triples, binding ++ t.binding)
      } else {
        NoMatch
      }
    case NoMatch => 
      NoMatch
  }
}

object NoMatch extends Match {
  def &(m : Match) = NoMatch
}

object SparqlXMLResults {
	def toXML(plan : Plan[_], m : DistCollection[Match]) = {
		<sparql xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:xs="http://www.w3.org/2001/XMLSchema#" xmlns="http://www.w3.org/2005/sparql-results#">
			<head>
				{head(plan)}
			</head>
		</sparql>
	}

	private def head(plan : Plan[_]) = {
		plan.vars.map { v => <variable name={v}></variable> }
	}

	def verify(resultFile : java.net.URL, plan : Plan[DistCollection[Match]], result : DistCollection[Match]) {
		val sparqlXML = XML.load(resultFile)
		val head = sparqlXML \\ "head"
		require((head \ "variable").size == plan.vars.size)
		for(variable <- (head \ "variable")) {
			require(plan.vars.contains((variable \ "@name").text))
		}
	}

	def verify(resultFile : java.net.URL, plan : Plan[Boolean], result : Boolean) {
		val sparqlXML = XML.load(resultFile)
		(sparqlXML \ "boolean").text match {
			case "true" => require(result)
			case "false" => require(!result)
			case other => throw new IllegalArgumentException("Bad Boolean value " + other)
		}
	}
}
