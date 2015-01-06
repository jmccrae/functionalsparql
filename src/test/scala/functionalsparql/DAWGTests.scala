package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.sparql.core.Quad
import java.io.File
import java.net.URL
import org.apache.jena.riot.RDFDataMgr
import org.scalatest._
import scala.collection.JavaConversions._
import scala.xml.XML

class DAWGTests extends FlatSpec with Matchers {
  val testsToSkip = Set(
    // TODO: File Bug Report @ Jena
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/basic/manifest#term-6",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/basic/manifest#term-7",
    // TODO: File Bug Report @ DAWG
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/cast/manifest#cast-bool",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/cast/manifest#cast-dec",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional-filter/manifest#dawg-optional-filter-005-not-simplified",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#opt-filter-2",
    // Unapproved
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#date-1")

  for(dir <- new File("src/test/resources/test-suite-archive/data-r2/").listFiles() if dir.isDirectory) {
    for(file <- dir.listFiles() if file.getName() == "manifest.ttl") {
      val model = RDFDataMgr.loadModel("file:" + file.getPath())
      val testListTriples = model.listResourcesWithProperty(
        model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
          model.createResource("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#QueryEvaluationTest")
            )
      for(test <- testListTriples if !testsToSkip.exists(test.toString.contains(_))) {
      //for(test <- testListTriples if test.toString == "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/solution-seq/manifest#offset-1") {
      //for(test <- testListTriples if test.toString.contains("opt-filter-2") || test.toString.contains("nested-opt-1") || test.toString.contains("filter-scope-1")) {
        val expResult = test.getProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#result")).getObject().asResource()
          val queries = test.getProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#action")).getObject().asResource().
            listProperties(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-query#query")).map { st =>
              st.getObject().asResource()
            }
            val dataset = Option(test.getProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#action")).getObject().asResource().
              getProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-query#data"))).map(_.getObject().asResource())
            val givenGraphDataset = test.getProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#action")).getObject().asResource().
              listProperties(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-query#graphData")).map(_.getObject().asNode())
            test.toString should "execute" in {
              for(query <- queries) {
                //println("Processing " + query.toString)
                val plan = functionalsparql.processQuery(scala.io.Source.fromURL(query.toString)(scala.io.Codec.UTF8).getLines.mkString("\n"), query.toString)
                val data = dataset match {
                  case Some(ds) => RDFDataMgr.loadModel(
                    ds.getURI()).listStatements().toList.map(s => new Quad(Quad.defaultGraphIRI, s.asTriple))
                  case None => Nil
                } 
                val graphDataset = Set() ++ givenGraphDataset ++ plan.graphs ++ plan.namedGraphs
                val graphData = graphDataset flatMap { ds =>
                    val model = RDFDataMgr.loadModel(ds.getURI())
                    model.listStatements().toList.map(s => new Quad(ds, s.asTriple))
                }
                require(dataset == None || data.size() > 0)
                plan match {
                  case p : SelectPlan =>
                    val result = p.execute(new SimpleDistCollection(data ++ graphData))
                    if(expResult.getURI().endsWith(".srx")) {
                      SparqlXMLResults.verify(new URL(expResult.getURI()), p, result)
                    } else if(expResult.getURI().endsWith(".ttl") 
                      || expResult.getURI().endsWith(".rdf")) {
                      SparqlTurtleResults.verify(new URL(expResult.getURI()), p, result)
                    } else {
                      fail("Unknown results" + expResult)
                    }
                  case p : AskPlan => 
                    val result = p.execute(new SimpleDistCollection(data ++ graphData))
                    if(expResult.getURI().endsWith(".srx")) {
                      SparqlXMLResults.verify(new URL(expResult.getURI()), p, result)
                    } else if(expResult.getURI().endsWith(".ttl")) {
                      result should be (expResult.getURI().endsWith("true.ttl"))
                      //SparqlTurtleResults.verify(new URL(expResult.getURI()), p, result)
                    } else {
                      fail("Unknown results" + expResult)
                    }

                  case _ =>
                    System.err.println("TODO")
                    }
                }
            }//}
        }
      }
    }


    object SparqlTurtleResults {
      def verify(resultFile : java.net.URL, plan : Plan[DistCollection[Match]],
          resultDC : DistCollection[Match]) {
        val sparqlTTL = RDFDataMgr.loadGraph(resultFile.toString())
        val statements = sparqlTTL.find(null, 
          NodeFactory.createURI("http://www.w3.org/2001/sw/DataAccess/tests/result-set#solution"),
            null)
        val results = new scala.collection.mutable.ArrayBuffer[Match]() ++ resultDC.toIterable
        
        for(result <- statements) {
          val bindingStats = sparqlTTL.find(result.getObject(),
            NodeFactory.createURI("http://www.w3.org/2001/sw/DataAccess/tests/result-set#binding"),
            null)
          val r2 = Seq() ++ (for(binding <- bindingStats) yield {
            val k = sparqlTTL.find(binding.getObject(),
              NodeFactory.createURI("http://www.w3.org/2001/sw/DataAccess/tests/result-set#variable"),
              null).next().getObject().getLiteralLexicalForm()
            val v = sparqlTTL.find(binding.getObject(),
              NodeFactory.createURI("http://www.w3.org/2001/sw/DataAccess/tests/result-set#value"),
              null).next().getObject()
            k -> v
          })
          val resultMatch = results.find { result =>
            result.binding.size == r2.size &&
            r2.forall { 
              case (k, v) => 
                result.binding.contains(k) && (result.binding(k) == v ||
                  result.binding(k).isBlank() && v.isBlank())
            }
          }

          resultMatch match {
            case Some(s) => results.remove(results.indexOf(s))
            case None => fail("not found:" + r2)
          }
        }
        for(result <- results) {
          fail("not a result:" + result.toString)
        }
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
        plan.vars.size should be ((head \ "variable").size)
        for(variable <- (head \ "variable")) {
          require(plan.vars.contains((variable \ "@name").text))
      }
      val results = new scala.collection.mutable.ArrayBuffer[Match]() ++ resultDC.toIterable
      
      for(result <- (sparqlXML \ "results" \ "result")) {
        val r2 = (for(binding <- (result \ "binding")) yield {
          val k = (binding \ "@name").text 
          val v = (if((binding \ "uri").size > 0) {
            (binding \ "uri").text
          } else if((binding \ "bnode").size > 0) {
            "_:"
          } else if((binding \ "literal").size > 0) {
            if((binding \ "literal" \ "@{http://www.w3.org/XML/1998/namespace}lang").size > 0) {
              "\"" + (binding \ "literal").text + "\"@" + (binding \ "literal" \ "@{http://www.w3.org/XML/1998/namespace}lang").text
            } else if((binding \ "literal" \ "@datatype").size > 0) {
              "\"" + (binding \ "literal").text + "\"^^" + (binding \ "literal" \ "@datatype").text
            } else {
              "\"" + (binding \ "literal").text + "\""//^^http://www.w3.org/2001/XMLSchema#string" 
            }
          } else {
            throw new RuntimeException()
          })
          k -> v
        })
        val resultMatch = results.find { result =>
          result.binding.size == r2.size &&
          r2.forall { 
            case (k, v) => 
              result.binding.contains(k) && (result.binding(k).toString == v ||
                (v == "_:" && result.binding(k).isBlank()))
          }
        }

        resultMatch match {
          case Some(s) => results.remove(results.indexOf(s))
          case None => fail("not found:" + r2)
        }
      }
      for(result <- results) {
        fail("not a result:" + result.toString)
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
}
