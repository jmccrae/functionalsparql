package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.Node
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

object Expressions {
  def anyFromNode(n : Node) : Any = if(n.isLiteral()) {
    try {
      n.getLiteralValue() match {
        case i : java.lang.Integer => 
          i.intValue()
        case d : java.lang.Double => 
          d.doubleValue()
        case f : java.lang.Float => 
          f.floatValue()
        case d : java.math.BigDecimal =>
          BigDecimal(d)
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
            case "http://www.w3.org/2001/XMLSchema#dateTime" => parseIso8601Date(tv.lexicalValue)
            case "http://www.w3.org/2001/XMLSchema#boolean" => tv.lexicalValue == "true"
            case _ => BadCast(tv, new SparqlEvaluationException("Unsupported datatype"))
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

  def parseIso8601Date(iso8601string : String) = {
    val s = iso8601string.replace("Z", "+00:00")
    val s2 = try {
      s.substring(0, 22) + s.substring(23)
    } catch {
      case e : IndexOutOfBoundsException =>
        throw new java.text.ParseException("Invalid length", 0)
    }
    new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").parse(s2)
  }

  def writeIso8601Date(date : Date) = {
    val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
    formatter.setTimeZone(java.util.TimeZone.getTimeZone("GMT"))
    formatter.format(date).replaceAll("\\+0000$","Z")
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
  }
}

trait Expression {
  def vars : Set[String]
  def yieldValue(m : Match) : Any
}

trait Expression1 extends Expression {
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
        x2 match {
          case i : Int =>
            handlers.get(classOf[Int]).map(_.apply(i)).getOrElse(Error)
          case f : Float =>
            handlers.get(classOf[Float]).map(_.apply(f)).getOrElse(Error)
          case d : Double =>
            handlers.get(classOf[Double]).map(_.apply(d)).getOrElse(Error)
          case d : BigDecimal => 
            handlers.get(classOf[BigDecimal]).map(_.apply(d)).getOrElse(Error)
          case s : String =>
            handlers.get(classOf[String]).map(_.apply(s)).getOrElse(Error)
          case dt : Date =>
            handlers.get(classOf[Date]).map(_.apply(dt)).getOrElse(Error)
          case n : Node =>
            handlers.get(classOf[Node]).map(_.apply(n)).getOrElse(Error)
          case l : LangString =>
            handlers.get(classOf[LangString]).map(_.apply(l)).getOrElse(Error)
        }
    }
  }
}

case class LiteralExpression(value : Any) extends Expression {
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

case class Bound(varName : String) extends Expression {
  def vars = Set()
  def yieldValue(m : Match) = m.binding.get(varName) match {
    case Some(null) => False
    case Some(_) => True
    case None => False
  }
}

case class IsURI(expr : Expression) extends Expression1 {
  handler { (x : Node) => Logic(x.isURI()) }
}

case class IsBLANK(expr : Expression) extends Expression1 {
  handler { (x : Node) => Logic(x.isBlank()) }
}

case class IsLiteral(expr : Expression) extends Expression1 {
  handler { (x : Node) => Logic(x.isLiteral()) }
  handler { (x : Int) => True }
  handler { (x : Float) => True }
  handler { (x : Double) => True }
  handler { (x : BigDecimal) => True }
  handler { (x : Date) => True }
  handler { (x : Boolean) => True }
  handler { (x : String) => True }
  handler { (x : LangString) => True }
}

case class Str(expr : Expression) extends Expression1 {
  handler { (x : Node) => x.toString }
  handler { (x : Int) => x.toString }
  handler { (x : Float) => x.toString }
  handler { (x : Double) => x.toString }
  handler { (x : BigDecimal) => x.toString }
  handler { (x : Date) => Expressions.writeIso8601Date(x) }
  handler { (x : Boolean) => x.toString }
  handler { (x : String) => x.toString }
  handler { (x : LangString) => x.string }
}


