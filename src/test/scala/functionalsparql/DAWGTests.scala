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
  // TODO: File Bug Report @ Jena
  val testsToSkip = Set(
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/basic/manifest#term-6",
  "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/basic/manifest#term-7",
  // To decide
  "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/cast/manifest#cast-dT",
  "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/cast/manifest#cast-dec",
  "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/cast/manifest#cast-bool",
  "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#join-scope-1",
  "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-12")

  for(dir <- new File("src/test/resources/test-suite-archive/data-r2/").listFiles() if dir.isDirectory) {
    for(file <- dir.listFiles() if file.getName() == "manifest.ttl") {
      val model = RDFDataMgr.loadModel("file:" + file.getPath())
      val testListTriples = model.listResourcesWithProperty(
        model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
          model.createResource("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#QueryEvaluationTest")
            )
      for(test <- testListTriples if !testsToSkip.contains(test.toString)) {
      //for(test <- testListTriples if test.toString == "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/bnode-coreference/manifest#dawg-bnode-coref-001") {
        val expResult = test.getProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#result")).getObject().asResource()
          val queries = test.getProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#action")).getObject().asResource().
            listProperties(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-query#query")).map { st =>
              st.getObject().asResource()
            }
            val dataset = Option(test.getProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#action")).getObject().asResource().
              getProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-query#data"))).map(_.getObject().asResource())
                val graphDataset = Option(test.getProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#action")).getObject().asResource().
                  getProperty(model.createProperty("http://www.w3.org/2001/sw/DataAccess/tests/test-query#graphData"))).map(_.getObject().asResource())
                    test.toString should "execute" in {
                      for(query <- queries) {
                        //println("Processing " + query.toString)
                        val plan = functionalsparql.processQuery(scala.io.Source.fromURL(query.toString)(scala.io.Codec.UTF8).getLines.mkString("\n"))
                        //println(plan)
                        val data = dataset match {
                          case Some(ds) => RDFDataMgr.loadModel(
                            ds.getURI()).listStatements().toList.map(s => new Quad(Quad.defaultGraphIRI, s.asTriple))
                          case None => Nil
                        } 
                        val graphData = graphDataset match {
                          case Some(ds) => 
                            val model = RDFDataMgr.loadModel(ds.getURI())
                            model.listStatements().toList.map(s => new Quad(NodeFactory.createURI(ds.getURI()), s.asTriple))
                          case None =>
                            Nil
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
          var size = 0
          for(result <- statements) {
            size += 1
          }
          resultDC.toIterable.size should be (size)
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
        val results = resultDC.toIterable
        if((sparqlXML \ "results" \ "result").size != results.size) {
          //println("Failed result")
          //println(plan)
          //println(resultFile)
          //for(r <- results) {
          //  println(r)
          //}
          //println((sparqlXML \ "results" \ "result").size)
          results.size should be ((sparqlXML \ "results" \ "result").size)
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
      }
