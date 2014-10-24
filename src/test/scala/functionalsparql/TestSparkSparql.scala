package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.{NodeFactory,Triple}
import com.hp.hpl.jena.sparql.core.Var
import org.scalatest.{mock => mock_package, _}
import org.apache.spark.{Partition, SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.log4j.{Logger, Level}
import org.mockito.Mockito._
import org.mockito.Matchers.any
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer


class functionalsparqlTest extends FlatSpec with ShouldMatchers {
  import functionalsparql._

  val doSparkTests = false

  def withColl(foo : DistCollection[Triple] => Unit) = {

    if(!doSparkTests) {
      foo(functionalsparql.parseRDD(new SimpleDistCollection(scala.io.Source.fromFile("src/test/resources/test.nt").getLines.toSeq), 
        "file:src/test/resources/test.nt"))
    } else {
      val conf = new SparkConf().setAppName("Simple test").setMaster("local")
      val sc = new SparkContext(conf)
      try {
        foo(functionalsparql.parseRDD(new SparkDistCollection(sc.textFile("src/test/resources/test.nt")), 
          "file:src/test/resources/test.nt"))
      } finally {
        sc.stop()
      }

    }
      
  }
  
  "triple rdd" should "work" in {
    withColl { rdd =>
      rdd.count() should be (1000)
    }
  }

  "query parser" should "parse simple SPARQL query" in {
    val query = "select * where { ?s a <http://www.w3.org/ns/dcat#Dataset> }"
    val result = functionalsparql.processQuery(query)
    result should be (SelectPlan(List("s"),SimpleFilter(
      new Triple(Var.alloc("s"), 
        NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
        NodeFactory.createURI("http://www.w3.org/ns/dcat#Dataset")
          ))))
  }

  "query translator" should "produce results for simple SPARQL query" in {
    withColl { rdd =>
      val query = "select * where { ?s a <http://www.w3.org/ns/dcat#CatalogRecord> }"
      val plan = functionalsparql.processQuery(query)
      val result = plan.execute(rdd).asInstanceOf[DistCollection[Match]]
      val count = result.count()
      count should be (49)
    }
  }

  "query parser" should "parser complex SPARQL query" in {
    val query = """
select * where { ?s a <http://www.w3.org/ns/dcat#CatalogRecord> ;
  <http://purl.org/dc/terms/title> "Fihrist"
}"""
    val result = functionalsparql.processQuery(query)
    result should be (SelectPlan(List("s"), JoinFilter(
      SimpleFilter(new Triple(Var.alloc("s"), 
        NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
        NodeFactory.createURI("http://www.w3.org/ns/dcat#CatalogRecord"))),
      SimpleFilter(new Triple(Var.alloc("s"),
        NodeFactory.createURI("http://purl.org/dc/terms/title"),
        NodeFactory.createLiteral("Fihrist")
        )))))
  }

  "query translator" should "produce results for join SPARQL query" in {
    withColl { rdd =>
      val query = """
select * where { ?s a <http://www.w3.org/ns/dcat#CatalogRecord> ;
  <http://purl.org/dc/terms/title> "Fihrist"
}"""
      val plan = functionalsparql.processQuery(query)
      val result = plan.execute(rdd).asInstanceOf[DistCollection[Match]]
      val count = result.count()
      count should not be (0)
    }
  }

  "query parser" should "parser optional SPARQL query" in {
    val query = """
select * where { ?s a <http://www.w3.org/ns/dcat#CatalogRecord> .
  optional { ?s <http://purl.org/dc/terms/title> "Fihrist" }
}"""
    val result = functionalsparql.processQuery(query)
    result should be (SelectPlan(List("s"), JoinFilter(
      SimpleFilter(new Triple(Var.alloc("s"), 
        NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
        NodeFactory.createURI("http://www.w3.org/ns/dcat#CatalogRecord"))),
      OptionalFilter(SimpleFilter(new Triple(Var.alloc("s"),
        NodeFactory.createURI("http://purl.org/dc/terms/title"),
        NodeFactory.createLiteral("Fihrist")
        ))))))
  }

  "query translator" should "produce results for optional SPARQL query" in {
    withColl { rdd =>
      val query = """
select * where { ?s a <http://www.w3.org/ns/dcat#CatalogRecord> .
  optional { ?s <http://purl.org/dc/terms/title> "Fihrist" }
}"""
      val plan = functionalsparql.processQuery(query)
      val result = plan.execute(rdd).asInstanceOf[DistCollection[Match]]
      val count = result.count()
      count should not be (0)
    }
  }

  "query parser" should "parse union SPARQL query" in {
    val query = """
select * where { ?s a <http://www.w3.org/ns/dcat#CatalogRecord> .
  { ?s <http://purl.org/dc/terms/title> "Fihrist" } union { ?s <http://purl.org/dc/terms/creator> "olac2cmdi.xsl" }
}"""
    val result = functionalsparql.processQuery(query)
    result should be (SelectPlan(List("s"), JoinFilter(
      SimpleFilter(new Triple(Var.alloc("s"), 
        NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
        NodeFactory.createURI("http://www.w3.org/ns/dcat#CatalogRecord"))),
      UnionFilter(Seq(SimpleFilter(new Triple(Var.alloc("s"),
        NodeFactory.createURI("http://purl.org/dc/terms/title"),
        NodeFactory.createLiteral("Fihrist")
        )),SimpleFilter(new Triple(Var.alloc("s"),
        NodeFactory.createURI("http://purl.org/dc/terms/creator"),
        NodeFactory.createLiteral("olac2cmdi.xsl")
        ))
      )))))
  }

  "query translator" should "produce results for union SPARQL query" in {
    withColl { rdd =>
      val query = """
select * where { ?s a <http://www.w3.org/ns/dcat#CatalogRecord> .
  { ?s <http://purl.org/dc/terms/title> "Fihrist" } union { ?s <http://purl.org/dc/terms/creator> "olac2dmci.xsl" }
}"""
      val plan = functionalsparql.processQuery(query)
      val result = plan.execute(rdd).asInstanceOf[DistCollection[Match]]
      val count = result.count()
      count should not be (0)
    }
  }

"query parser" should "parse filter SPARQL query" in {
    val query = """
select * where { ?s <http://purl.org/dc/terms/title> ?l . filter(regex(?l,"Fih.*"))
}"""
    val result = functionalsparql.processQuery(query)
    result should be (SelectPlan(List("s","l"), 
      JoinExprFilter(
        SimpleFilter(new Triple(Var.alloc("s"), 
          NodeFactory.createURI("http://purl.org/dc/terms/title"),
          Var.alloc("l"))),
        RegexExpressionFilter(
          VariableExpressionFilter(Var.alloc("l")),
          ValueFilter("Fih.*"),
          NullExpressionFilter)
      )))
  }

  "query translator" should "produce results for filter SPARQL query" in {
    withColl { rdd =>
      val query = """
select * where { ?s <http://purl.org/dc/terms/title> ?l . filter(regex(?l,"Fih.*"))
}"""
      val plan = functionalsparql.processQuery(query)
      val result = plan.execute(rdd).asInstanceOf[DistCollection[Match]]
      val count = result.count()
      count should not be (0)
    }
  }
  "query translator" should "produce results for filter SPARQL query (2)" in {
    withColl { rdd =>
      val query = """
select * where { ?s a <http://www.w3.org/ns/dcat#CatalogRecord> .
   ?s <http://purl.org/dc/terms/title> ?l . filter(regex(?l,"Fih.*"))
}"""
      val plan = functionalsparql.processQuery(query)
      val result = plan.execute(rdd).asInstanceOf[DistCollection[Match]]
      val count = result.count()
      count should not be (0)
    }
  }

  "query translator" should "produce results for not exists SPARQL query" in {
   withColl { rdd =>
      val query = """
select * where { ?s a <http://www.w3.org/ns/dcat#CatalogRecord> .
   filter(not exists { ?s <http://purl.org/dc/terms/title> "Fihrist" })
}"""
      val plan = functionalsparql.processQuery(query)
      val result = plan.execute(rdd).asInstanceOf[DistCollection[Match]]
      val count = result.count()
      count should not be (0)
    } 
  }

  "query translator" should "produce good plan for filter query" in {
    val query = """
    PREFIX t: <http://www.w3.org/2001/sw/DataAccess/tests/data/TypePromotion/tP-0#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
ASK
 WHERE { t:double1 rdf:value ?l .
         t:decimal1 rdf:value ?r .
         FILTER ( datatype(?l + ?r) = xsd:double ) }"""
      val plan = functionalsparql.processQuery(query)
  }

}
