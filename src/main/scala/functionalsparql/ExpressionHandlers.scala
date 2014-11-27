package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.{Node, NodeFactory}
import com.hp.hpl.jena.sparql.core.Var
import java.util.regex.Pattern
import org.joda.time.{DateTime, LocalDate, LocalTime}

case class UnaryNot(expr : Expression) extends Expression1 {
  handler { (x : EffectiveBooleanValue) => !x.value }
}

case class UnaryPlus(expr : Expression) extends Expression1 {
  handler { (x : Double) => x }
  handler { (x : Int) => x }
  handler { (x : Float) => x }
  handler { (x : BigDecimal) => x }
}

case class UnaryNeg(expr : Expression) extends Expression1 {
  handler { (x : Double) => -x }
  handler { (x : Int) => -x }
  handler { (x : Float) => -x }
  handler { (x : BigDecimal) => -x }
}

case class Bound(expr : Expression) extends Expression {
  def vars = Set()
  def yieldValue(m : Match) = {
    expr match {
      case VariableExpression(name) =>   
        m.binding.get(name) match {
          case Some(null) => False
          case Some(_) => 
            True
          case None => False
        }
      case _ =>
        Error
    }
  }
}

case class IsURI(expr : Expression) extends Expression1 {
  handler { (x : Node) => Logic(x.isURI()) }
  handler { (x : Int) => False }
  handler { (x : Float) => False }
  handler { (x : Double) => False }
  handler { (x : BigDecimal) => False }
  handler { (x : DateTime) => False }
  handler { (x : LocalDate) => False }
  handler { (x : Logic) => False }
  handler { (x : String) => False }
  handler { (x : LangString) => False }
  handler { (x : UnsupportedLiteral) => False }
  handler { (x : ErrorLiteral) => False }
}

case class IsBLANK(expr : Expression) extends Expression1 {
  handler { (x : Node) => Logic(x.isBlank()) }
  handler { (x : Int) => False }
  handler { (x : Float) => False }
  handler { (x : Double) => False }
  handler { (x : BigDecimal) => False }
  handler { (x : DateTime) => False }
  handler { (x : LocalDate) => False }
  handler { (x : Logic) => False }
  handler { (x : String) => False }
  handler { (x : LangString) => False }
  handler { (x : UnsupportedLiteral) => False }
  handler { (x : ErrorLiteral) => False }
}

case class IsLiteral(expr : Expression) extends Expression1 {
  handler { (x : Node) => Logic(x.isLiteral()) }
  handler { (x : Int) => True }
  handler { (x : Float) => True }
  handler { (x : Double) => True }
  handler { (x : BigDecimal) => True }
  handler { (x : DateTime) => True }
  handler { (x : LocalDate) => True }
  handler { (x : Logic) => True }
  handler { (x : String) => True }
  handler { (x : LangString) => True }
  handler { (x : UnsupportedLiteral) => True }
  handler { (x : ErrorLiteral) => True }
}

case class Str(expr : Expression) extends Expression1 {
  handler { (x : RawNode) => 
    if(x.node.isLiteral()) {
      x.node.getLiteralLexicalForm()
    } else {
      x.node.toString
    }
  }
  handler { (x : Node) => x.toString }
  handler { (x : Int) => x.toString }
  handler { (x : Float) => x.toString }
  handler { (x : Double) => x.toString }
  handler { (x : BigDecimal) => x.toString }
  handler { (x : DateTime) => x.toString() }
  handler { (x : LocalDate) => x.toString() }
  handler { (x : Logic) => x.toString }
  handler { (x : String) => x.toString }
  handler { (x : LangString) => x.string }
  handler { (x : UnsupportedLiteral) => x.string }
  handler { (x : ErrorLiteral) => x.string }
}

// Avoid name clash with Jena
case class LangExpression(expr : Expression) extends Expression1 {
  handler { (x : LangString) => x.lang.toLowerCase }
  handler { (x : String) => "" }
  handler { (x : UnsupportedLiteral) => "" }
  handler { (x : ErrorLiteral) => "" }
  handler { (x : Int) => "" }
  handler { (x : Float) => "" }
  handler { (x : Double) => "" }
  handler { (x : BigDecimal) => "" }
  handler { (x : Logic) => "" }
  handler { (x : DateTime) => "" }
  handler { (x : LocalDate) => "" }
}

case class Datatype(expr : Expression) extends Expression1 {
  handler { (x : Int) => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#integer") }
  handler { (x : Float) => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#float") }
  handler { (x : Double) => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#double") }
  handler { (x : BigDecimal) => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#decimal") }
  handler { (x : DateTime) => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#dateTime") }
  handler { (x : LocalDate) => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#date") }
  handler { (x : Logic) => {
      if(x != Error) { NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#boolean") } else { Error } 
    }
  }
  handler { (x : String) => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#string") }
  //handler { (x : LangString) => NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString") }
  handler { (x : UnsupportedLiteral) => x.datatype }
  handler { (x : ErrorLiteral) => x.datatype }
}

case class LogicAnd(expr1 : Expression, expr2 : Expression) extends Expression2 {
  handler { (x : EffectiveBooleanValue, y : EffectiveBooleanValue) => 
    x.value && y.value }
}

case class LogicOr(expr1 : Expression, expr2 : Expression) extends Expression2 {
  handler { (x : EffectiveBooleanValue, y : EffectiveBooleanValue) => 
    x.value || y.value }
}

case class SameTerm(expr1 : Expression, expr2 : Expression) extends Expression2 {
  handler { (x : RawNode, y : RawNode) => Logic(x == y) }
}

case class Equals(expr1 : Expression, expr2 : Expression) extends Expression2 {
  handler { (x : Int, y : Int) => Logic(x == y) }
  handler { (x : Int, y : Float) => Logic(x == y) }
  handler { (x : Int, y : Double) => Logic(x == y) }
  handler { (x : Int, y : BigDecimal) => Logic(x == y) }
  handler { (x : Int, y : String) => False }
  handler { (x : Int, y : LangString) => False }
  handler { (x : Int, y : Logic) => y >= False }
  handler { (x : Int, y : DateTime) => False }
  handler { (x : Int, y : LocalDate) => False }
  handler { (x : Int, y : Node) => False }
  handler { (x : Int, y : ErrorLiteral) => False }
  handler { (x : Float, y : Int) => Logic(x == y) }
  handler { (x : Float, y : Float) => Logic(x == y) }
  handler { (x : Float, y : Double) => Logic(x == y) }
  handler { (x : Float, y : BigDecimal) => Logic(x == y) }
  handler { (x : Float, y : String) => False }
  handler { (x : Float, y : LangString) => False }
  handler { (x : Float, y : Logic) => y >= False }
  handler { (x : Float, y : DateTime) => False }
  handler { (x : Float, y : LocalDate) => False }
  handler { (x : Float, y : Node) => False }
  handler { (x : Float, y : ErrorLiteral) => False}
  handler { (x : Double, y : Int) => Logic(x == y) }
  handler { (x : Double, y : Float) => Logic(x == y) }
  handler { (x : Double, y : Double) => Logic(x == y) }
  handler { (x : Double, y : BigDecimal) => Logic(x == y) }
  handler { (x : Double, y : String) => False }
  handler { (x : Double, y : LangString) => False }
  handler { (x : Double, y : Logic) => y >= False }
  handler { (x : Double, y : LocalDate) => False }
  handler { (x : Double, y : Node) => False }
  handler { (x : Double, y : ErrorLiteral) => False}
  handler { (x : BigDecimal, y : Int) => Logic(x == y) }
  handler { (x : BigDecimal, y : Float) => Logic(x == y) }
  handler { (x : BigDecimal, y : Double) => Logic(x == y) }
  handler { (x : BigDecimal, y : BigDecimal) => Logic(x == y) }
  handler { (x : BigDecimal, y : String) => False }
  handler { (x : BigDecimal, y : LangString) => False }
  handler { (x : BigDecimal, y : Logic) => y >= False }
  handler { (x : BigDecimal, y : DateTime) => False }
  handler { (x : BigDecimal, y : LocalDate) => False }
  handler { (x : BigDecimal, y : Node) => False }
  handler { (x : BigDecimal, y : ErrorLiteral) => False}
  handler { (x : LangString, y : LangString) => Logic(x == y) }
  handler { (x : LangString, y : Int) => False }
  handler { (x : LangString, y : Float) => False }
  handler { (x : LangString, y : Double) => False }
  handler { (x : LangString, y : BigDecimal) => False }
  handler { (x : LangString, y : String) => False }
  handler { (x : LangString, y : Logic) => y >= False }
  handler { (x : LangString, y : DateTime) => False }
  handler { (x : LangString, y : LocalDate) => False }
  handler { (x : LangString, y : Node) => False }
  handler { (x : LangString, y : UnsupportedLiteral) => False}
  handler { (x : LangString, y : ErrorLiteral) => False}
  handler { (x : String, y : LangString) => False }
  handler { (x : String, y : String) => Logic(x == y) }
  handler { (x : String, y : Int) => False }
  handler { (x : String, y : Float) => False }
  handler { (x : String, y : Double) => False }
  handler { (x : String, y : BigDecimal) => False }
  handler { (x : String, y : Logic) => y >= False }
  handler { (x : String, y : DateTime) => False }
  handler { (x : String, y : LocalDate) => False }
  handler { (x : String, y : Node) => False }
  handler { (x : Logic, y : Logic) => if(x == Error || y == Error) {
      Error
    } else {
      Logic(x == y) 
    }
  }
  handler { (x : Logic, y : Int) => x >= False }
  handler { (x : Logic, y : Float) => x >= False }
  handler { (x : Logic, y : Double) => x >= False }
  handler { (x : Logic, y : BigDecimal) => x >= False }
  handler { (x : Logic, y : String) => x >= False }
  handler { (x : Logic, y : LangString) => x >= False }
  handler { (x : Logic, y : DateTime) => x >= False }
  handler { (x : Logic, y : LocalDate) => x >= False }
  handler { (x : Logic, y : Node) => x >= False }
  handler { (x : Logic, y : ErrorLiteral) => x >= False}
  handler { (x : UnsupportedLiteral, y : UnsupportedLiteral) => 
    if(x == y)  {
      True
    } else {
      Error
    }
  }
  handler { (x : DateTime, y : DateTime) => Logic(x == y) }
  handler { (x : DateTime, y : LocalDate) => False }
  handler { (x : DateTime, y : Int) => False }
  handler { (x : DateTime, y : Float) => False }
  handler { (x : DateTime, y : Double) => False }
  handler { (x : DateTime, y : BigDecimal) => False }
  handler { (x : DateTime, y : String) => False }
  handler { (x : DateTime, y : LangString) => False }
  handler { (x : DateTime, y : Logic) => y >= False }
  handler { (x : DateTime, y : Node) => False }
  handler { (x : DateTime, y : ErrorLiteral) => False}
  handler { (x : LocalDate, y : LocalDate) => Logic(x == y) }
  handler { (x : LocalDate, y : DateTime) => False }
  handler { (x : LocalDate, y : Int) => False }
  handler { (x : LocalDate, y : Float) => False }
  handler { (x : LocalDate, y : Double) => False }
  handler { (x : LocalDate, y : BigDecimal) => False }
  handler { (x : LocalDate, y : String) => False }
  handler { (x : LocalDate, y : LangString) => False }
  handler { (x : LocalDate, y : Logic) => y >= False }
  handler { (x : LocalDate, y : Node) => False }
  handler { (x : LocalDate, y : ErrorLiteral) => False}
  handler { (x : Node, y : Node) => Logic(x == y) }
  handler { (x : Node, y : Int) => False }
  handler { (x : Node, y : Float) => False }
  handler { (x : Node, y : Double) => False }
  handler { (x : Node, y : BigDecimal) => False }
  handler { (x : Node, y : String) => False }
  handler { (x : Node, y : LangString) => False }
  handler { (x : Node, y : DateTime) => False }
  handler { (x : Node, y : LocalDate) => False }
  handler { (x : Node, y : Logic) => y >= False }
  handler { (x : Node, y : UnsupportedLiteral) => False }
  handler { (x : Node, y : ErrorLiteral) => False }
  handler { (x : ErrorLiteral, y : ErrorLiteral) => if(x == y) { True } else { Error } }
  handler { (x : ErrorLiteral, y : Int) => False }
  handler { (x : ErrorLiteral, y : Float) => False }
  handler { (x : ErrorLiteral, y : Double) => False }
  handler { (x : ErrorLiteral, y : BigDecimal) => False }
  handler { (x : ErrorLiteral, y : LangString) => False }
  handler { (x : ErrorLiteral, y : DateTime) => False }
  handler { (x : ErrorLiteral, y : LocalDate) => False }
  handler { (x : ErrorLiteral, y : Logic) => y >= False }
  handler { (x : ErrorLiteral, y : Node) => False }
  handler { (x : UnsupportedLiteral, y : LangString) => False }
  handler { (x : UnsupportedLiteral, y : Logic) => y >= False }
  handler { (x : UnsupportedLiteral, y : Node) => False }
}

case class LessThan(expr1 : Expression, expr2 : Expression) extends Expression2 {
  handler { (x : Int, y : Int) => Logic(x < y) }
  handler { (x : Int, y : Float) => Logic(x < y) }
  handler { (x : Int, y : Double) => Logic(x < y) }
  handler { (x : Int, y : BigDecimal) => Logic(x < y) }
  handler { (x : Float, y : Int) => Logic(x < y) }
  handler { (x : Float, y : Float) => Logic(x < y) }
  handler { (x : Float, y : Double) => Logic(x < y) }
  handler { (x : Float, y : BigDecimal) => Logic(y >= x) }
  handler { (x : Double, y : Int) => Logic(x < y) }
  handler { (x : Double, y : Float) => Logic(x < y) }
  handler { (x : Double, y : Double) => Logic(x < y) }
  handler { (x : Double, y : BigDecimal) => Logic(x < y) }
  handler { (x : BigDecimal, y : Int) => Logic(x < y) }
  handler { (x : BigDecimal, y : Float) => Logic(x < y) }
  handler { (x : BigDecimal, y : Double) => Logic(x < y) }
  handler { (x : BigDecimal, y : BigDecimal) => Logic(x < y) }
  handler { (x : Logic, y : Logic) => 
    if(x == False && y == True) {
      True
    } else if(x != Error && y != Error) {
      False
    } else {
      Error
    }
  }
  handler { (x : DateTime, y : DateTime) => Logic(x.isBefore(y)) }
  handler { (x : LocalDate, y : LocalDate) => Logic(x.isBefore(y)) }
  handler { (x : String, y : String) => Logic(x < y) }
}


case class GreaterThan(expr1 : Expression, expr2 : Expression) extends Expression2 {
  handler { (x : Int, y : Int) => Logic(x > y) }
  handler { (x : Int, y : Float) => Logic(x > y) }
  handler { (x : Int, y : Double) => Logic(x > y) }
  handler { (x : Int, y : BigDecimal) => Logic(x > y) }
  handler { (x : Float, y : Int) => Logic(x > y) }
  handler { (x : Float, y : Float) => Logic(x > y) }
  handler { (x : Float, y : Double) => Logic(x > y) }
  handler { (x : Float, y : BigDecimal) => Logic(y <= x) }
  handler { (x : Double, y : Int) => Logic(x > y) }
  handler { (x : Double, y : Float) => Logic(x > y) }
  handler { (x : Double, y : Double) => Logic(x > y) }
  handler { (x : Double, y : BigDecimal) => Logic(x > y) }
  handler { (x : BigDecimal, y : Int) => Logic(x > y) }
  handler { (x : BigDecimal, y : Float) => Logic(x > y) }
  handler { (x : BigDecimal, y : Double) => Logic(x > y) }
  handler { (x : BigDecimal, y : BigDecimal) => Logic(x > y) }
  handler { (x : Logic, y : Logic) => 
    if(x == True && y == False) {
      True
    } else if(x != Error && y != Error) {
      False
    } else {
      Error
    }
  }
  handler { (x : DateTime, y : DateTime) => Logic(x.isAfter(y)) }
  handler { (x : LocalDate, y : LocalDate) => Logic(x.isAfter(y)) }
  handler { (x : String, y : String) => Logic(x > y) }
}


case class LessThanEq(expr1 : Expression, expr2 : Expression) extends Expression2 {
  handler { (x : Int, y : Int) => Logic(x <= y) }
  handler { (x : Int, y : Float) => Logic(x <= y) }
  handler { (x : Int, y : Double) => Logic(x <= y) }
  handler { (x : Int, y : BigDecimal) => Logic(x <= y) }
  handler { (x : Float, y : Int) => Logic(x <= y) }
  handler { (x : Float, y : Float) => Logic(x <= y) }
  handler { (x : Float, y : Double) => Logic(x <= y) }
  handler { (x : Float, y : BigDecimal) => Logic(y > x) }
  handler { (x : Double, y : Int) => Logic(x <= y) }
  handler { (x : Double, y : Float) => Logic(x <= y) }
  handler { (x : Double, y : Double) => Logic(x <= y) }
  handler { (x : Double, y : BigDecimal) => Logic(x <= y) }
  handler { (x : BigDecimal, y : Int) => Logic(x <= y) }
  handler { (x : BigDecimal, y : Float) => Logic(x <= y) }
  handler { (x : BigDecimal, y : Double) => Logic(x <= y) }
  handler { (x : BigDecimal, y : BigDecimal) => Logic(x <= y) }
  handler { (x : Logic, y : Logic) => 
    if(x == False && y == True || (x == y && x != Error)) {
      True
    } else if(x != Error && y != Error) {
      False
    } else {
      Error
    }
  }
  handler { (x : DateTime, y : DateTime) => Logic(x.isBefore(y) || x.isEqual(y)) }
  handler { (x : LocalDate, y : LocalDate) => Logic(x.isBefore(y) || x.isEqual(y)) }
  handler { (x : String, y : String) => Logic(x <= y) }
}


case class GreaterThanEq(expr1 : Expression, expr2 : Expression) extends Expression2 {
  handler { (x : Int, y : Int) => Logic(x >= y) }
  handler { (x : Int, y : Float) => Logic(x >= y) }
  handler { (x : Int, y : Double) => Logic(x >= y) }
  handler { (x : Int, y : BigDecimal) => Logic(x >= y) }
  handler { (x : Float, y : Int) => Logic(x >= y) }
  handler { (x : Float, y : Float) => Logic(x >= y) }
  handler { (x : Float, y : Double) => Logic(x >= y) }
  handler { (x : Float, y : BigDecimal) => Logic(y < x) }
  handler { (x : Double, y : Int) => Logic(x >= y) }
  handler { (x : Double, y : Float) => Logic(x >= y) }
  handler { (x : Double, y : Double) => Logic(x >= y) }
  handler { (x : Double, y : BigDecimal) => Logic(x >= y) }
  handler { (x : BigDecimal, y : Int) => Logic(x >= y) }
  handler { (x : BigDecimal, y : Float) => Logic(x >= y) }
  handler { (x : BigDecimal, y : Double) => Logic(x >= y) }
  handler { (x : BigDecimal, y : BigDecimal) => Logic(x >= y) }
  handler { (x : Logic, y : Logic) => 
    if(x == True && y == False || (x == y && x != Error)) {
      True
    } else if(x != Error && y != Error) {
      False
    } else {
      Error
    }
  }
  handler { (x : DateTime, y : DateTime) => Logic(x.isAfter(y) || x.isEqual(y)) }
  handler { (x : LocalDate, y : LocalDate) => Logic(x.isAfter(y) || x.isEqual(y)) }
  handler { (x : String, y : String) => Logic(x >= y) }
}

case class Multiply(expr1 : Expression, expr2 : Expression) extends Expression2 {
  handler { (x : Int, y : Int) => x * y }
  handler { (x : Int, y : Float) => x * y }
  handler { (x : Int, y : Double) => x * y }
  handler { (x : Int, y : BigDecimal) => x * y }
  handler { (x : Float, y : Int) => x * y }
  handler { (x : Float, y : Float) => x * y }
  handler { (x : Float, y : Double) => x * y }
  handler { (x : Float, y : BigDecimal) => (y * x).toFloat }
  handler { (x : Double, y : Int) => x * y }
  handler { (x : Double, y : Float) => x * y }
  handler { (x : Double, y : Double) => x * y }
  handler { (x : Double, y : BigDecimal) => (x * y).toDouble }
  handler { (x : BigDecimal, y : Int) => x * y }
  handler { (x : BigDecimal, y : Float) => (x * y).toFloat }
  handler { (x : BigDecimal, y : Double) => (x * y).toDouble }
  handler { (x : BigDecimal, y : BigDecimal) => x * y }
}

case class Divide(expr1 : Expression, expr2 : Expression) extends Expression2 {
  handler { (x : Int, y : Int) => x / y }
  handler { (x : Int, y : Float) => x / y }
  handler { (x : Int, y : Double) => x / y }
  handler { (x : Int, y : BigDecimal) => x / y }
  handler { (x : Float, y : Int) => x / y }
  handler { (x : Float, y : Float) => x / y }
  handler { (x : Float, y : Double) => x / y }
  handler { (x : Float, y : BigDecimal) => (y / x).toFloat }
  handler { (x : Double, y : Int) => x / y }
  handler { (x : Double, y : Float) => x / y }
  handler { (x : Double, y : Double) => x / y }
  handler { (x : Double, y : BigDecimal) => (x / y).toDouble }
  handler { (x : BigDecimal, y : Int) => x / y }
  handler { (x : BigDecimal, y : Float) => (x / y).toFloat }
  handler { (x : BigDecimal, y : Double) => (x / y).toDouble }
  handler { (x : BigDecimal, y : BigDecimal) => x / y }
}

case class Add(expr1 : Expression, expr2 : Expression) extends Expression2 {
  handler { (x : Int, y : Int) => x + y }
  handler { (x : Int, y : Float) => x + y }
  handler { (x : Int, y : Double) => x + y }
  handler { (x : Int, y : BigDecimal) => x + y }
  handler { (x : Float, y : Int) => x + y }
  handler { (x : Float, y : Float) => x + y }
  handler { (x : Float, y : Double) => x + y }
  handler { (x : Float, y : BigDecimal) => (y + x).toFloat }
  handler { (x : Double, y : Int) => x + y }
  handler { (x : Double, y : Float) => x + y }
  handler { (x : Double, y : Double) => x + y }
  handler { (x : Double, y : BigDecimal) => (x + y).toDouble }
  handler { (x : BigDecimal, y : Int) => x + y }
  handler { (x : BigDecimal, y : Float) => (x + y).toFloat }
  handler { (x : BigDecimal, y : Double) => (x + y).toDouble }
  handler { (x : BigDecimal, y : BigDecimal) => x + y }
}

case class Subtract(expr1 : Expression, expr2 : Expression) extends Expression2 {
  handler { (x : Int, y : Int) => x - y }
  handler { (x : Int, y : Float) => x - y }
  handler { (x : Int, y : Double) => x - y }
  handler { (x : Int, y : BigDecimal) => x - y }
  handler { (x : Float, y : Int) => x - y }
  handler { (x : Float, y : Float) => x - y }
  handler { (x : Float, y : Double) => x - y }
  handler { (x : Float, y : BigDecimal) => (y - x).toFloat }
  handler { (x : Double, y : Int) => x - y }
  handler { (x : Double, y : Float) => x - y }
  handler { (x : Double, y : Double) => x - y }
  handler { (x : Double, y : BigDecimal) => (x - y).toDouble }
  handler { (x : BigDecimal, y : Int) => x - y }
  handler { (x : BigDecimal, y : Float) => (x - y).toFloat }
  handler { (x : BigDecimal, y : Double) => (x - y).toDouble }
  handler { (x : BigDecimal, y : BigDecimal) => x - y }
}

case class LangMatches(expr1 : Expression, expr2 : Expression) extends Expression2 {
  def checkLang(tag : String, range : String) = if(range == "*") {
    Logic(tag != "")
  } else if (tag.contains("-") && !range.contains("-")) {
    Logic(tag.substring(0,tag.indexOf("-")).toLowerCase == range.toLowerCase)
  } else {
    Logic(tag.toLowerCase == range.toLowerCase)
  }

  handler { (x : String, y : String) => checkLang(x, y) }
  handler { (x : String, y : LangString) => checkLang(x, y.string) }
}

case class Regex(expr1 : Expression, expr2 : Expression, 
    expr3 : Option[Expression]) extends Expression {
  def vars = expr1.vars ++ expr2.vars ++ expr3.map(_.vars).getOrElse(Set())
  def checkFlag(flags : String, flag : String, result : Int) = {
    if(flags != null && flags.contains(flag)) {
      result
    } else {
      0
    }
  }
  def doRegex(input : String, pattern : String, flags : String) = {
    val flag = checkFlag(flags, "s", Pattern.DOTALL) |
      checkFlag(flags, "m", Pattern.MULTILINE) |
      checkFlag(flags, "i", Pattern.CASE_INSENSITIVE) |
      checkFlag(flags, "x", Pattern.COMMENTS)
    Pattern.compile(pattern, flag).matcher(input).find()
  }
  def yieldValue(m : Match) = {
    def valToString(expr : Expression) = expr.yieldValue(m) match {
      case s : String => 
        s
      case LangString(s, _) => 
        s
      case n : Node => 
        Expressions.anyFromNode(n) match {
          case s : String =>
            s
          case LangString(s, _) =>
            s
          case _ =>
            null
        }
      case _ =>
        null
    }
    val input = valToString(expr1)
    val pattern = valToString(expr2)
    val flags = expr3.map(valToString(_)).getOrElse("")
    if(input != null && pattern != null && flags != null) {
      Logic(doRegex(input, pattern, flags))
    } else {
      Error
    }
  }
}

case class CastInt(expr : Expression) extends Expression1 {
  handler { (i : Int) => i }
  handler { (f : Float) => f.toInt }
  handler { (d : Double) => d.toInt }
  handler { (bd : BigDecimal) => bd.toInt }
  handler { (l : Logic) => 
    if(l == True) {
      1
    } else if(l == False) {
      0
    } else {
      l
    }
  }
  handler { (str : String) => 
    try { 
      str.toInt 
    } catch {
      case x : NumberFormatException => 
        Error
    }
  }
  handler { (ls : LangString) => 
    try { 
      ls.string.toInt
    } catch {
      case x : NumberFormatException => 
        Error
    }
  }
}

case class CastBoolean(expr : Expression) extends Expression1 {
  handler { (ebv : EffectiveBooleanValue) => ebv.value } // EBV does the hard work :)
}

case class CastFloat(expr : Expression) extends Expression1 {
  handler { (i : Int) => i.toFloat }
  handler { (f : Float) => f }
  handler { (d : Double) => d.toFloat }
  handler { (bd : BigDecimal) => bd.toFloat }
  handler { (l : Logic) => 
    if(l == True) {
      1.0f
    } else if(l == False) {
      0.0f
    } else {
      l
    }
  }
  handler { (str : String) => 
    try { 
      str.toFloat 
    } catch {
      case x : NumberFormatException => 
        Error
    }
  }
  handler { (ls : LangString) => 
    try { 
      ls.string.toFloat 
    } catch {
      case x : NumberFormatException => 
        Error
    }
  }
}


case class CastDouble(expr : Expression) extends Expression1 {
  handler { (i : Int) => i.toDouble }
  handler { (f : Float) => f.toDouble }
  handler { (d : Double) => d }
  handler { (bd : BigDecimal) => bd.toDouble }
  handler { (l : Logic) => 
    if(l == True) {
      1.0
    } else if(l == False) {
      0.0
    } else {
      l
    }
  }
  handler { (str : String) => 
    try { 
      str.toDouble 
    } catch {
      case x : NumberFormatException => 
        Error
    }
  }
  handler { (ls : LangString) =>
    try { 
      ls.string.toDouble 
    } catch {
      case x : NumberFormatException => 
        Error
    }
  }
}

case class CastDecimal(expr : Expression) extends Expression1 {
  handler { (i : Int) => BigDecimal(i) }
  handler { (f : Float) => BigDecimal(f) }
  handler { (d : Double) => BigDecimal(d) }
  handler { (bd : BigDecimal) => bd }
  handler { (l : Logic) => 
    if(l == True) {
      BigDecimal(1.0)
    } else if(l == False) {
      BigDecimal(0.0)
    } else {
      l
    }
  }
  handler { (str : String) => 
    try { 
      BigDecimal(str)
    } catch {
      case x : NumberFormatException => 
        Error
    }
  }
  handler { (ls : LangString) => 
    try { 
      BigDecimal(ls.string) 
    } catch {
      case x : NumberFormatException => 
        Error
    }
  }
}

case class CastDateTime(expr : Expression) extends Expression1 {
  handler { (s : String) => 
    try {
      if(s.matches("\\-?\\d{4}\\-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?([+-]\\d{2}:\\d{2}|Z)?")) {
        DateTime.parse(s)
      } else {
        Error
      }
    } catch {
      case x : IllegalArgumentException =>
        Error
    }
  }//Option(Expressions.parseIso8601Date(s)).getOrElse(Error) }
  handler { (s : LangString) => 
    try {
      if(s.string.matches("\\-?\\d{4}\\-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?([+-]\\d{2}:\\d{2}|Z)?")) {
        DateTime.parse(s.string)
      } else {
        Error
      }
    } catch {
      case x : IllegalArgumentException =>
        Error
    }
  }//Option(Expressions.parseIso8601Date(s.string)).getOrElse(Error) }
  handler { (d : DateTime) => d }
  handler { (d : LocalDate) => d.toDateTime(LocalTime.MIDNIGHT) }
}

case class VariableExpression(varName : String) extends Expression {
  def vars = Set(varName)
  def yieldValue(m : Match) = {
    m.binding.get(varName).getOrElse(Error)
  }
}

object TrueFilter extends Expression {
  def vars = Set()
  def yieldValue(m : Match) = True
}

