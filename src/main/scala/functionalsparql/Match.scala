package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.{Node, Triple}
import java.io.File
import scala.xml.XML

//sealed trait Match {
//  def &(m : Match) : Match
//}

case class Match(triples : Seq[Triple], binding : Map[String, Node]) {
  def compatible(m : Match) = (binding.keys.toSet & m.binding.keys.toSet) forall { k =>
    binding(k) == m.binding(k)
  }

  def &(m : Match) = {
  	require(compatible(m))
  	Match(triples ++ m.triples, binding ++ m.binding)
  }
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

	def verify(resultFile : java.net.URL, plan : Plan[DistCollection[Match]], resultDC : DistCollection[Match]) {
		val sparqlXML = XML.load(resultFile)
		val head = sparqlXML \\ "head"
		require((head \ "variable").size == plan.vars.size)
		for(variable <- (head \ "variable")) {
			require(plan.vars.contains((variable \ "@name").text))
		}
		val results = resultDC.toIterable
		if((sparqlXML \ "results" \ "result").size != results.size) {
			println("Failed result")
			println(resultFile)
			for(r <- results) {
				println(r)
			}
			println((sparqlXML \ "results" \ "result").size)
			require(false)
		}
		for(result <- (sparqlXML \ "results" \ "result")) {

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
