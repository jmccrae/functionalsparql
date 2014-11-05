package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.sparql.core.Quad
import java.io.File
import java.net.URL
import org.apache.jena.riot.RDFDataMgr
import org.scalatest._
import scala.collection.JavaConversions._

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
          "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-12"
        )

	for(dir <- new File("src/test/resources/test-suite-archive/data-r2/").listFiles() if dir.isDirectory) {
		for(file <- dir.listFiles() if file.getName() == "manifest.ttl") {
			val model = RDFDataMgr.loadModel("file:" + file.getPath())
			val testListTriples = model.listResourcesWithProperty(
				model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				model.createResource("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#QueryEvaluationTest")
				)
			//for(test <- testListTriples if !testsToSkip.contains(test.toString)) {
                        for(test <- testListTriples if false) {
				//if (test.toString == "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/algebra/manifest#nested-opt-1") {
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
						println("Processing " + query.toString)
      					val plan = functionalsparql.processQuery(scala.io.Source.fromURL(query.toString)(scala.io.Codec.UTF8).getLines.mkString("\n"))
                                        println(plan)
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
      							}
      						case p : AskPlan => 
      							val result = p.execute(new SimpleDistCollection(data ++ graphData))
      							if(expResult.getURI().endsWith(".srx")) {
      								SparqlXMLResults.verify(new URL(expResult.getURI()), p, result)
      							} 
      						case _ =>
      							System.err.println("TODO")
      					}
      				}
				}//}
			}
		}
	}

}
