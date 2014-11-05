package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.vocabulary.XSD
import org.scalatest._

class TestExpression extends WordSpec with Matchers {
  "Tri-value Logic" when {
    "True" should {
      "be negatable" in {
        !True should be (False)
      }
      "define &&" in {
        (True && True) should be (True)
        (True && False) should be (False)
        (True && Error) should be (Error)
      }
      "define ||" in {
        (True || True) should be (True)
        (True || False) should be (True)
        (True || Error) should be (True)
      }
    }
    "False" should {
      "be negatable" in {
        !False should be (True)
      }
      "define &&" in {
        (False && True) should be (False)
        (False && False) should be (False)
        (False && Error) should be (False)
      }
      "define ||" in {
        (False || True) should be (True)
        (False || False) should be (False)
        (False || Error) should be (Error)
      }
    }
    "Error" should {
      "be negatable" in {
        !Error should be (Error)
      }
      "define &&" in {
        (Error && True) should be (Error)
        (Error && False) should be (False)
        (Error && Error) should be (Error)
      }
      "define ||" in {
        (Error || True) should be (True)
        (Error || False) should be (Error)
        (Error || Error) should be (Error)
      }
    }
  }

  "UnaryNot" should {
    "work for logic" in {
      UnaryNot(LiteralExpression(True)).yieldValue(null) should be (False)
    }
    "work for string" in {
      UnaryNot(LiteralExpression("")).yieldValue(null) should be (True)
      UnaryNot(LiteralExpression("foo")).yieldValue(null) should be (False)
    }
    "not work for node" in {
      UnaryNot(LiteralExpression(NodeFactory.createURI("file:test"))).yieldValue(null) should be (Error)
    }
  }

  "UnaryPlus" should {
    "work for doubles" in {
      UnaryPlus(LiteralExpression(2.3)).yieldValue(null) should be (2.3)
    }
    "work for literal" in {
      UnaryPlus(LiteralExpression(
        NodeFactory.createLiteral("2.3", 
          NodeFactory.getType(XSD.decimal.getURI())))).
      yieldValue(null) should be (BigDecimal(2.3))
    }
    "not work for node" in {
      UnaryPlus(LiteralExpression(NodeFactory.createURI("file:test"))).
        yieldValue(null) should be (Error)
    }
  }

  "UnaryNeg" should {
    "negate" in {
      UnaryNeg(LiteralExpression(2.3)).yieldValue(null) should be (-2.3)
    }
  }

  "Bound" should {
    "detect a unbound variable" in {
      Bound("s").yieldValue(Match(Set(),Map())) should be (False)
    }
    "detect a bound variable" in {
      Bound("s").yieldValue(Match(Set(),
        Map("s" -> NodeFactory.createURI("file:test")))) should be (True)
    }
  }

  "isURI" should {
    "detect a URI" in {
      IsURI(LiteralExpression(NodeFactory.createURI("file:test"))).
        yieldValue(null) should be (True)
    }
  }

  "isBLANK" should {
    "detect a Blank" in {
      IsBLANK(LiteralExpression(NodeFactory.createAnon())).
        yieldValue(null) should be (True)
    }
  }

  "isLiteral" should {
    "detect a literal node" in {
      IsLiteral(LiteralExpression(NodeFactory.createLiteral("foo"))).
        yieldValue(null) should be (True)
    }
    "detect a literal value" in {
      IsLiteral(LiteralExpression("foo")).
        yieldValue(null) should be (True)
    }
  }

  "Str" should {
    "not change string" in {
      Str(LiteralExpression("foo")).
        yieldValue(null) should be ("foo")
    }
    "change numeric" in {
      Str(LiteralExpression(2.0)).
        yieldValue(null) should be ("2.0")
    }
    "yield URI for resource" in {
      Str(LiteralExpression(NodeFactory.createURI("file:test"))).
        yieldValue(null) should be ("file:test")
    }
    "yield content for lang string" in {
      Str(LiteralExpression(LangString("foo","en"))).
        yieldValue(null) should be ("foo")
    }
    "yield date in ISO 8601 date" in {
      Str(LiteralExpression(new java.util.Date(0l))).
        yieldValue(null) should be ("1970-01-01T00:00:00Z")
    }
    "yield decimals as numbers" in {
      Str(LiteralExpression(BigDecimal(2))).
        yieldValue(null) should be ("2")
    }
  }
}
