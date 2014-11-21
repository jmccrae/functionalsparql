package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.{Node, NodeFactory}
import com.hp.hpl.jena.sparql.core.Var
import java.util.regex.Pattern
import java.util.Date

sealed trait Logic {
  def unary_! : Logic
  def &&(l : Logic) : Logic
  def ||(l : Logic) : Logic
}

object Logic {
  def apply(x : Boolean) = if(x) {
    True
  } else {
    False
  }
}

object True extends Logic {
  def unary_! = False
  def &&(l : Logic) = l match {
    case True => True
    case False => False
    case Error => Error
  }
  def ||(l : Logic) = l match {
    case True => True
    case False => True
    case Error => True
  }
  override def toString = "true"
}

object False extends Logic {
  def unary_! = True
  def &&(l : Logic) = l match {
    case True => False
    case False => False
    case Error => False
  }
  def ||(l : Logic) = l match {
    case True => True
    case False => False
    case Error => Error
  }
  override def toString = "false"
}

object Error extends Logic {
  def unary_! = Error
  def &&(l : Logic) = l match {
    case True => Error
    case False => False
    case Error => Error
  }
  def ||(l : Logic) = l match {
    case True => True
    case False => Error
    case Error => Error
  }
  override def toString = "error"
}

case class LangString(string : String, lang : String)

case class UnsupportedLiteral(string : String, datatype : Node)

object Expressions {
  def anyFromNode(n : Node) : Any = if(n.isLiteral()) {
    try {
      n.getLiteralValue() match {
        case i : java.lang.Integer => 
          if(n.getLiteralDatatype() != null && 
            n.getLiteralDatatype().getURI() == "http://www.w3.org/2001/XMLSchema#decimal") {
            BigDecimal(i)
          } else {
            i
          }
        case d : java.lang.Double => 
          if(n.getLiteralDatatype() != null && 
            n.getLiteralDatatype().getURI() == "http://www.w3.org/2001/XMLSchema#decimal") {
            BigDecimal(d)
          } else {
            d
          }
        case f : java.lang.Float => 
          if(n.getLiteralDatatype() != null && 
            n.getLiteralDatatype().getURI() == "http://www.w3.org/2001/XMLSchema#decimal") {
            BigDecimal(f.doubleValue())
          } else {
            f
          }
        case d : java.math.BigDecimal =>
          BigDecimal(d)
        case d : java.math.BigInteger =>
          if(n.getLiteralDatatype() != null && 
            n.getLiteralDatatype().getURI() == "http://www.w3.org/2001/XMLSchema#decimal") {
            BigDecimal(d.intValue())
          } else {
            d.intValue()
          }
         case b : java.lang.Boolean => 
          if(b) { True } else { False }
        case d : BigDecimal => 
          d
        case s : String => 
          if(n.getLiteralLanguage() != null && n.getLiteralLanguage() != "") {
            LangString(s, n.getLiteralLanguage().toLowerCase())
          } else {
            s
          }
        case dt : com.hp.hpl.jena.datatypes.xsd.XSDDateTime => 
          dt.asCalendar().getTime()
        case tv : com.hp.hpl.jena.datatypes.BaseDatatype.TypedValue => 
          tv.datatypeURI match {
            case "http://www.w3.org/2001/XMLSchema#string" => tv.lexicalValue
            case "http://www.w3.org/2001/XMLSchema#float" => tv.lexicalValue.toFloat
            case "http://www.w3.org/2001/XMLSchema#double" => tv.lexicalValue.toDouble
            case "http://www.w3.org/2001/XMLSchema#decimal" => BigDecimal(tv.lexicalValue)
            case "http://www.w3.org/2001/XMLSchema#integer" => tv.lexicalValue.toInt
            case "http://www.w3.org/2001/XMLSchema#dateTime" => Option(parseIso8601Date(tv.lexicalValue)).getOrElse(Error)
            case "http://www.w3.org/2001/XMLSchema#boolean" => tv.lexicalValue == "true"
            case _ => 
              UnsupportedLiteral(tv.lexicalValue, NodeFactory.createURI(tv.datatypeURI))
          }
        case o => throw new SparqlEvaluationException(o.getClass().getName())
      }
    } catch {
      case x : com.hp.hpl.jena.datatypes.DatatypeFormatException =>
        Error
    }
  } else if(n.isURI()) {
    n
  } else if(n.isVariable()) {
    Error
  } else if(n.isBlank()) {
    n
  } else {
    throw new SparqlEvaluationException("What is this: " + n)
  }

  // returns null if error!
  def parseIso8601Date(iso8601string : String) = {
    val s = iso8601string.replace("Z", "+00:00")
    try {
      val s2 = s.substring(0, 22) + s.substring(23)
      new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").parse(s2)
    } catch {
      case e : IndexOutOfBoundsException =>
        null
    }
  }

  def writeIso8601Date(date : Date) = {
    val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
    formatter.setTimeZone(java.util.TimeZone.getTimeZone("GMT"))
    formatter.format(date).replaceAll("\\+0000$","Z")
  }

  def sparqlClass(a : Any) = a match {
    case i : Int => 
      classOf[Int]
    case f : Float =>
      classOf[Float]
    case d : Double =>
      classOf[Double]
    case b : BigDecimal =>
      classOf[BigDecimal]
    case s : String =>
      classOf[String]
    case l : LangString =>
      classOf[LangString]
    case u : UnsupportedLiteral =>
      classOf[UnsupportedLiteral]
    case d : Date =>
      classOf[Date]
    case n : Node =>
      classOf[Node]
    case l : Logic =>
      classOf[Logic]
    case _ =>
      throw new SparqlEvaluationException("An invalid type was generated " + a.getClass())
  }
}

class EffectiveBooleanValue(val value : Logic)

object EffectiveBooleanValue {
  def apply(x : Any) : EffectiveBooleanValue = x match {
    case true => 
      new EffectiveBooleanValue(True)
    case false =>
      new EffectiveBooleanValue(False)
    case l : Logic =>
      new EffectiveBooleanValue(l)
    case "" =>
      new EffectiveBooleanValue(False)
    case s : String =>
      new EffectiveBooleanValue(True)
    case LangString("",_) =>
      new EffectiveBooleanValue(False)
    case l : LangString =>
      new EffectiveBooleanValue(True)
    case d : Double =>
      if(d == 0.0 || d == Double.NaN) {
        new EffectiveBooleanValue(False)
      } else {
        new EffectiveBooleanValue(True)
      }
    case i : Int =>
      if(i == 0) {
        new EffectiveBooleanValue(False)
      } else {
        new EffectiveBooleanValue(True)
      }
    case f : Float =>
      if(f == 0 || f == Float.NaN) {
        new EffectiveBooleanValue(False)
      } else {
        new EffectiveBooleanValue(True)
      }
    case f : BigDecimal =>
      if(f == 0) {
        new EffectiveBooleanValue(False)
      } else {
        new EffectiveBooleanValue(True)
      }
    case n : Node =>
      if(n.isLiteral()) {
        EffectiveBooleanValue(Expressions.anyFromNode(n))
      } else {
        new EffectiveBooleanValue(Error)
      }
    case ul : UnsupportedLiteral =>
      EffectiveBooleanValue(Error)
  }
}

trait Expression {
  def vars : Set[String]
  def yieldValue(m : Match) : Any
  def applyTo(rdd : DistCollection[Match]) = rdd.filter(applyOnce)
  def applyOnce(m : Match) = { 
    val v = yieldValue(m)
    EffectiveBooleanValue(v).value == True
  }
}

trait Expression1 extends Expression {
  import Expressions._
  def expr : Expression
  private var handlers = Map[Class[_], Function1[Any,Any]]()
  def vars = expr.vars

  def handler[A](foo : A => Any)(implicit manifest : Manifest[A]) {
    handlers += (manifest.runtimeClass -> { x : Any => foo(x.asInstanceOf[A]) })
  }

  def yieldValue(m : Match) = {
    // Get value from sub-expression
    val x = expr.yieldValue(m)
    // Call EBV calls in preference
    handlers.get(classOf[EffectiveBooleanValue]) match {
      case Some(h) =>
        h.apply(EffectiveBooleanValue(x))
      case None =>
        // Convert result if possible
        val x2 = x match {
          case n : Node => 
            Expressions.anyFromNode(n)
          case _ =>
            x
        }
        handlers.get(sparqlClass(x2)).map(_.apply(x2)).getOrElse(Error)
    }
  }
}

trait Expression2 extends Expression {
  import Expressions._
  def expr1 : Expression
  def expr2 : Expression
  private var handlers = Map[(Class[_], Class[_]), Function2[Any, Any, Any]]()
  def vars = expr1.vars ++ expr2.vars

  def handler[A,B](foo : (A, B) => Any)(implicit manifest : Manifest[A], 
    manifest2 : Manifest[B]) {
    handlers += ((manifest.runtimeClass, manifest2.runtimeClass) -> 
      { (x : Any, y : Any) => foo(x.asInstanceOf[A], y.asInstanceOf[B]) })
  }

  def yieldValue(m : Match) = {
    // Get value from sub-expression
    val x = expr1.yieldValue(m)
    val y = expr2.yieldValue(m)
    // Call EBV calls in preference
    handlers.get(
      (classOf[EffectiveBooleanValue], classOf[EffectiveBooleanValue])
    ) match {
      case Some(h) =>
        h.apply(EffectiveBooleanValue(x), EffectiveBooleanValue(y))
      case None =>
        // Convert result if possible
        val x2 = x match {
          case n : Node => 
            Expressions.anyFromNode(n)
          case _ =>
            x
        }
        val y2 = y match {
          case n : Node => 
            Expressions.anyFromNode(n)
          case _ =>
            y
        }
        handlers.get(
          (sparqlClass(x2), sparqlClass(y2))
        ).map(_.apply(x2, y2)).getOrElse(Error)
    }
  }
}


case class LiteralExpression(value : Any) extends Expression {
  Expressions.sparqlClass(value)
  def vars = Set()
  def yieldValue(m : Match) = {
    value
  }
}

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
    expr.yieldValue(m) match {
      case v : Var =>   
        m.binding.get(v.getVarName()) match {
          case Some(null) => False
          case Some(_) => True
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
  handler { (x : Date) => False }
  handler { (x : Logic) => False }
  handler { (x : String) => False }
  handler { (x : LangString) => False }
  handler { (x : UnsupportedLiteral) => False }
}

case class IsBLANK(expr : Expression) extends Expression1 {
  handler { (x : Node) => Logic(x.isBlank()) }
  handler { (x : Int) => False }
  handler { (x : Float) => False }
  handler { (x : Double) => False }
  handler { (x : BigDecimal) => False }
  handler { (x : Date) => False }
  handler { (x : Logic) => False }
  handler { (x : String) => False }
  handler { (x : LangString) => False }
  handler { (x : UnsupportedLiteral) => False }
}

case class IsLiteral(expr : Expression) extends Expression1 {
  handler { (x : Node) => Logic(x.isLiteral()) }
  handler { (x : Int) => True }
  handler { (x : Float) => True }
  handler { (x : Double) => True }
  handler { (x : BigDecimal) => True }
  handler { (x : Date) => True }
  handler { (x : Logic) => True }
  handler { (x : String) => True }
  handler { (x : LangString) => True }
  handler { (x : UnsupportedLiteral) => True }
}

case class Str(expr : Expression) extends Expression1 {
  handler { (x : Node) => x.toString }
  handler { (x : Int) => x.toString }
  handler { (x : Float) => x.toString }
  handler { (x : Double) => x.toString }
  handler { (x : BigDecimal) => x.toString }
  handler { (x : Date) => Expressions.writeIso8601Date(x) }
  handler { (x : Logic) => x.toString }
  handler { (x : String) => x.toString }
  handler { (x : LangString) => x.string }
  handler { (x : UnsupportedLiteral) => x.string }
}

// Avoid name clash with Jena
case class LangExpression(expr : Expression) extends Expression1 {
  handler { (x : LangString) => x.lang.toLowerCase }
  handler { (x : String) => "" }
  handler { (x : UnsupportedLiteral) => "" }
  handler { (x : Int) => "" }
  handler { (x : Float) => "" }
  handler { (x : Double) => "" }
  handler { (x : BigDecimal) => "" }
  handler { (x : Logic) => "" }
  handler { (x : Date) => "" }
}

case class Datatype(expr : Expression) extends Expression1 {
  handler { (x : Int) => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#integer") }
  handler { (x : Float) => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#float") }
  handler { (x : Double) => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#double") }
  handler { (x : BigDecimal) => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#decimal") }
  handler { (x : Date) => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#dateTime") }
  handler { (x : Logic) => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#boolean") }
  handler { (x : String) => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#string") }
  //handler { (x : LangString) => NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString") }
  handler { (x : UnsupportedLiteral) => x.datatype }
}

case class LogicAnd(expr1 : Expression, expr2 : Expression) extends Expression2 {
  handler { (x : EffectiveBooleanValue, y : EffectiveBooleanValue) => 
    x.value && y.value }
}

case class LogicOr(expr1 : Expression, expr2 : Expression) extends Expression2 {
  handler { (x : EffectiveBooleanValue, y : EffectiveBooleanValue) => 
    x.value || y.value }
}

case class Equals(expr1 : Expression, expr2 : Expression, 
    sameTerm : Boolean = false) extends Expression2 {
  handler { (x : Int, y : Int) => Logic(x == y) }
  handler { (x : Int, y : Float) => Logic(x == y) }
  handler { (x : Int, y : Double) => Logic(x == y) }
  handler { (x : Int, y : BigDecimal) => Logic(x == y) }
  handler { (x : Float, y : Int) => Logic(x == y) }
  handler { (x : Float, y : Float) => Logic(x == y) }
  handler { (x : Float, y : Double) => Logic(x == y) }
  handler { (x : Float, y : BigDecimal) => Logic(x == y) }
  handler { (x : Double, y : Int) => Logic(x == y) }
  handler { (x : Double, y : Float) => Logic(x == y) }
  handler { (x : Double, y : Double) => Logic(x == y) }
  handler { (x : Double, y : BigDecimal) => Logic(x == y) }
  handler { (x : BigDecimal, y : Int) => Logic(x == y) }
  handler { (x : BigDecimal, y : Float) => Logic(x == y) }
  handler { (x : BigDecimal, y : Double) => Logic(x == y) }
  handler { (x : BigDecimal, y : BigDecimal) => Logic(x == y) }
  handler { (x : LangString, y : LangString) => Logic(x == y) }
  handler { (x : LangString, y : String) => False }
  handler { (x : String, y : LangString) => False }
  handler { (x : String, y : String) => Logic(x == y) }
  handler { (x : Logic, y : Logic) => Logic(x == y && x != Error && y != Error) }
  handler { (x : UnsupportedLiteral, y : UnsupportedLiteral) => 
    // SPARQL Query sec. 11.4.10-11
    if(x == y)  {
      True
    } else if(sameTerm) {
      False
    } else {
      Error
    }
  }
  handler { (x : Date, y : Date) => Logic(x == y) }
  handler { (x : Node, y : Node) => Logic(x == y) }
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
  handler { (x : Date, y : Date) => Logic(x.getTime() < y.getTime()) }
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
  handler { (x : Date, y : Date) => Logic(x.getTime() > y.getTime()) }
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
  handler { (x : Date, y : Date) => Logic(x.getTime() <= y.getTime()) }
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
  handler { (x : Date, y : Date) => Logic(x.getTime() >= y.getTime()) }
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
  handler { (s : String) => Option(Expressions.parseIso8601Date(s)).getOrElse(null) }
  handler { (s : LangString) => Option(Expressions.parseIso8601Date(s.string)).getOrElse(null) }
  handler { (d : Date) => d }
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
