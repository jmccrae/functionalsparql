package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.vocabulary.XSD
import java.util.Date
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
      Bound(LiteralExpression(Var.alloc("s"))).yieldValue(Match(Set(),Map())) should be (False)
    }
    "detect a bound variable" in {
      Bound(LiteralExpression(Var.alloc("s"))).yieldValue(Match(Set(),
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

  "Lang" should {
    "get the lang of a literal" in {
      LangExpression(LiteralExpression(LangString("foo", "EN"))).
        yieldValue(null) should be ("en")
    }
    "get the lang of a non-lang literal" in {
      LangExpression(LiteralExpression("foo")).
        yieldValue(null) should be ("")
    }
  }

  "Datatype" should {
    "get the datatype of an invalid literal" in {
      Datatype(LiteralExpression(
        NodeFactory.createLiteral("foo",
          NodeFactory.getType("file:example")))).
        yieldValue(null) should be (NodeFactory.createURI("file:example"))
    }
    "get the datatype of a string" in {
      Datatype(LiteralExpression("foo")).
        yieldValue(null) should be (NodeFactory.
          createURI("http://www.w3.org/2001/XMLSchema#string"))
    }
  }

  "And" should {
    "work for two booleans" in {
      LogicAnd(LiteralExpression(True), LiteralExpression(False)).
        yieldValue(null) should be (False)
    }
    "work with one effective value" in {
      LogicAnd(LiteralExpression(True), LiteralExpression("foo")).
        yieldValue(null) should be (True)
    }
    "consider errors" in {
      LogicAnd(LiteralExpression(True),
        LiteralExpression(Error)).
        yieldValue(null) should be (Error)
      LogicAnd(LiteralExpression(False), 
        LiteralExpression(Error)).
        yieldValue(null) should be (False)
    }
  }

  "Or" should {
    "work for two booleans" in {
      LogicOr(LiteralExpression(True), LiteralExpression(False)).
        yieldValue(null) should be (True)
    }
    "work with one effective value" in {
      LogicOr(LiteralExpression(False), LiteralExpression("foo")).
        yieldValue(null) should be (True)
    }
    "consider errors" in {
      LogicOr(LiteralExpression(True),
        LiteralExpression(Error)).
        yieldValue(null) should be (True)
      LogicOr(LiteralExpression(False), 
        LiteralExpression(Error)).
        yieldValue(null) should be (Error)
    }
  }

  "Equals" should {
    def test(x : Any, y : Any) = Equals(LiteralExpression(x), LiteralExpression(y)).
        yieldValue(null) should be (True)
    "be true for identical objects" in {
      test(1, 1)
      test(2.3, 2.3)
      test(2.0f, 2.0f)
      test(BigDecimal(100000),BigDecimal(100000))
      test("foo","foo")
      test(LangString("foo","en"), LangString("foo", "en"))
      test(False, False)
      val d = new Date()
      test(d, d)
      test(NodeFactory.createURI("foo:test"), NodeFactory.createURI("foo:test"))
      test(UnsupportedLiteral("foo", NodeFactory.createURI("file:test")),
        UnsupportedLiteral("foo", NodeFactory.createURI("file:test")))
    }
    "be true for convertible objects" in {
      test(1, 1.0)
      test(2.0, 2f)
      test("foo", NodeFactory.createLiteral("foo"))
    }
    "be error for incomparable types" in {
      Equals(LiteralExpression("3"), LiteralExpression(3)).
        yieldValue(null) should be (Error)
      Equals(LiteralExpression(UnsupportedLiteral("foo", NodeFactory.createURI("file:test"))),
        LiteralExpression(UnsupportedLiteral("foo", NodeFactory.createURI("file:test2")))).
        yieldValue(null) should be (Error)
    }
  }

  "Comparison" should {
    "work for similar objects" in {
      LessThan(LiteralExpression(1), LiteralExpression(2.0))
        .yieldValue(null) should be (True)
    }
    "fail for dissimlar objects" in {
      LessThan(LiteralExpression(1), LiteralExpression("2.0"))
        .yieldValue(null) should be (Error)
    }
    "work for dates" in {
      val d = new Date()
      val d2 = new Date(d.getTime() + 1)
      LessThan(LiteralExpression(d), LiteralExpression(d2))
        .yieldValue(null) should be (True)
    }
    "work for booleans" in {
      LessThan(LiteralExpression(False), LiteralExpression(True))
        .yieldValue(null) should be (True)
    }
  }

  "LangMatches" should {
    "work" in {
      LangMatches(LiteralExpression("en"), LiteralExpression("FR")).
        yieldValue(null) should be (False)

      LangMatches(LiteralExpression("fr"), LiteralExpression("FR")).
        yieldValue(null) should be (True)

      LangMatches(LiteralExpression("fr-BE"), LiteralExpression("FR")).
        yieldValue(null) should be (True)
    }
  }

  "Regex" should {
    "match simple expression" in {
      Regex(LiteralExpression("foo"), LiteralExpression("fo."), None).
        yieldValue(null) should be (True)
    }
    "match ignoring case" in {
      Regex(LiteralExpression("FOO"), LiteralExpression("fo."), Some(LiteralExpression("i"))).
        yieldValue(null) should be (True)
    }
    "match ignoring whitespace" in {
      Regex(LiteralExpression("foo"), LiteralExpression("f o."), Some(LiteralExpression("x"))).
        yieldValue(null) should be (True)
    }
    "account for case" in {
      Regex(LiteralExpression("abcDEFghiJKL"),LiteralExpression("GHI"),None).
        yieldValue(null) should be (False)
      Regex(LiteralExpression("ABCdefGHIjkl"),LiteralExpression("GHI"),None).
        yieldValue(null) should be (True)
      Regex(LiteralExpression("0123456789"),LiteralExpression("GHI"),None).
        yieldValue(null) should be (False)
    }
  }

  "Add" when {
    "adding decimal to decimal" should {
      "produce decimal" in {
        Add(LiteralExpression(BigDecimal(3)), LiteralExpression(BigDecimal(3))).
          yieldValue(null) should be (BigDecimal(6))
        Datatype(Add(LiteralExpression(BigDecimal(3)), LiteralExpression(BigDecimal(3)))).
          yieldValue(null) should be (NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#decimal"))
 
      }
    }
    "adding double to decimal" should {
      "produce double" in {
        Add(LiteralExpression(3.0), LiteralExpression(BigDecimal(3))).
          yieldValue(null) should be (6.0)
        Datatype(Add(LiteralExpression(3.0), LiteralExpression(BigDecimal(3)))).
          yieldValue(null) should be (NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#double"))
 
      }
    }
  }

  def anyTest[A](value : String, typ : String)(implicit manifest : Manifest[A]) = {
    ("given " + typ) should {
      ("produce " + manifest.runtimeClass.getName()) in {
        Expressions.anyFromNode(NodeFactory.createLiteral(value,
          NodeFactory.getType("http://www.w3.org/2001/XMLSchema#" 
            + typ))) shouldBe a[A]
      }
    }
  }

  "anyFromNode" when {
    anyTest[BigDecimal]("1","decimal")
    anyTest[java.lang.Float]("1","float")
    anyTest[java.lang.Double]("1","double")
    anyTest[Logic]("true","boolean")
    anyTest[java.util.Date]("2005-01-14T12:34:56","dateTime")
    anyTest[java.lang.Integer]("1","integer")
    anyTest[java.lang.Integer]("-1","nonPositiveInteger")
    anyTest[java.lang.Integer]("-1","negativeInteger")
    anyTest[java.lang.Integer]("1","long")
    anyTest[java.lang.Integer]("1","int")
    anyTest[java.lang.Integer]("1","short")
    anyTest[java.lang.Integer]("1","byte")
    anyTest[java.lang.Integer]("1","nonNegativeInteger")
    anyTest[java.lang.Integer]("1","unsignedLong")
    anyTest[java.lang.Integer]("1","unsignedInt")
    anyTest[java.lang.Integer]("1","unsignedShort")
    anyTest[java.lang.Integer]("1","unsignedByte")
    anyTest[java.lang.Integer]("1","positiveInteger")
  }
}
