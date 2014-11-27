package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.{Node, NodeFactory}
import com.hp.hpl.jena.sparql.core.Var
import java.util.regex.Pattern
import org.joda.time.DateTime
import org.joda.time.LocalDate

case class LangString(string : String, lang : String)

case class UnsupportedLiteral(string : String, datatype : Node) {
  require(datatype != null)
}

case class ErrorLiteral(string : String, datatype : Node) {
  require(datatype != null)
}

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
          try {
            if(n.getLiteralDatatypeURI() == "http://www.w3.org/2001/XMLSchema#date") {
              // XSD schema allows a time zone on dates but Joda does not
              LocalDate.parse(n.getLiteralLexicalForm().replaceAll(
                "([+-]\\d{2}:\\d{2}|Z)$", "")) 
            } else {
              DateTime.parse(n.getLiteralLexicalForm())
            //dt.asCalendar().getTime()
            }
          } catch {
            case x : IllegalArgumentException =>
              ErrorLiteral(n.getLiteralLexicalForm(), NodeFactory.createURI(n.getLiteralDatatype().toString))
          }
        case tv : com.hp.hpl.jena.datatypes.BaseDatatype.TypedValue => 
          tv.datatypeURI match {
            case "http://www.w3.org/2001/XMLSchema#string" => tv.lexicalValue
            case "http://www.w3.org/2001/XMLSchema#float" => tv.lexicalValue.toFloat
            case "http://www.w3.org/2001/XMLSchema#double" => tv.lexicalValue.toDouble
            case "http://www.w3.org/2001/XMLSchema#decimal" => BigDecimal(tv.lexicalValue)
            case "http://www.w3.org/2001/XMLSchema#integer" => tv.lexicalValue.toInt
            case "http://www.w3.org/2001/XMLSchema#dateTime" => try {
              DateTime.parse(tv.lexicalValue)
            } catch {
              case x : IllegalArgumentException =>
                Error
            }
              //Option(parseIso8601Date(tv.lexicalValue)).getOrElse(Error)
            case "http://www.w3.org/2001/XMLSchema#boolean" => tv.lexicalValue == "true"
            case _ => 
              UnsupportedLiteral(tv.lexicalValue, NodeFactory.createURI(tv.datatypeURI))
          }
        case o => throw new SparqlEvaluationException(o.getClass().getName())
      }
    } catch {
      case x : com.hp.hpl.jena.datatypes.DatatypeFormatException =>
        ErrorLiteral(n.getLiteralLexicalForm(), NodeFactory.createURI(n.getLiteralDatatype().toString))
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

  def nodeFromAny(any : Any) : Node = any match {
    case i : Int =>
      NodeFactory.createLiteral(i.toString, 
        NodeFactory.getType("http://www.w3.org/2001/XMLSchema#integer"))
    case f : Float =>
      NodeFactory.createLiteral(f.toString, 
        NodeFactory.getType("http://www.w3.org/2001/XMLSchema#float"))
    case d : Double =>
      NodeFactory.createLiteral(d.toString, 
        NodeFactory.getType("http://www.w3.org/2001/XMLSchema#double"))
    case bd : BigDecimal =>
      NodeFactory.createLiteral(bd.toString, 
        NodeFactory.getType("http://www.w3.org/2001/XMLSchema#decimal"))
    case s : String =>
      NodeFactory.createLiteral(s,
        NodeFactory.getType("http://www.w3.org/2001/XMLSchema#string"))
    case l : LangString =>
      NodeFactory.createLiteral(l.string, l.lang, false)
    case ul : UnsupportedLiteral =>
      NodeFactory.createLiteral(ul.string, NodeFactory.getType(ul.datatype.getURI()))
    case el : ErrorLiteral =>
      NodeFactory.createLiteral(el.string, NodeFactory.getType(el.datatype.getURI()))
    case d : DateTime => 
      NodeFactory.createLiteral(d.toString(),//writeIso8601DateTime(d),
        NodeFactory.getType("http://www.w3.org/2001/XMLSchema#dateTime"))
    case pd : LocalDate =>
      NodeFactory.createLiteral(pd.toString(),//writeIso8601Date(pd),
        NodeFactory.getType("http://www.w3.org/2001/XMLSchema#data"))
    case l : Logic =>
      NodeFactory.createLiteral(l.toString,
        NodeFactory.getType("http://www.w3.org/2001/XMLSchema#boolean"))
    case n : Node => n
  }

  // returns null if error!
/*  def parseIso8601Date(iso8601string : String) = {
    val s = iso8601string.replace("Z", "+00:00")
    try {
      val s2 = s.substring(0, 22) + s.substring(23)
      new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").parse(s2)
    } catch {
      case e : IndexOutOfBoundsException =>
        null
    }
  }

  def writeIso8601DateTime(date : DateTime) = {
    val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
    formatter.setTimeZone(java.util.TimeZone.getTimeZone("GMT"))
    formatter.format(date).replaceAll("\\+0000$","Z")
  }

  def writeIso8601Date(p : LocalDate) = {
    val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd")
    formatter.setTimeZone(java.util.TimeZone.getTimeZone("GMT"))
    formatter.format(p.d) 
  }*/

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
    case el : ErrorLiteral =>
      classOf[ErrorLiteral]
    case d : DateTime =>
      classOf[DateTime]
    case pd : LocalDate =>
      classOf[LocalDate]
    case n : Node =>
      classOf[Node]
    case l : Logic =>
      classOf[Logic]
    case _ =>
      throw new SparqlEvaluationException("An invalid type was generated " + a.getClass())
  }
}

case class RawNode(node : Node)

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
    case el : ErrorLiteral =>
      EffectiveBooleanValue(Error)
    case d : DateTime =>
      EffectiveBooleanValue(Error)
    case pd : LocalDate =>
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
    // Call raw processors first
    handlers.get(classOf[RawNode]) match {
      case Some(h) if x.isInstanceOf[Node] =>
        h(RawNode(x.asInstanceOf[Node]))
      case _ =>
        // Then Call EBV calls in preference
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
    // Look for a raw handler
    handlers.get(
      (classOf[RawNode], classOf[RawNode])
    ) match {
      case Some(h) =>
        x match {
          case xn : Node => y match {
            case yn : Node => h(RawNode(xn), RawNode(yn))
            case _ => 
              println("No conversion")
              Error
          }
          case _ =>
            println("No conversion")
            Error
        }
      case None =>
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
}


case class LiteralExpression(value : Any) extends Expression {
  Expressions.sparqlClass(value)
  def vars = Set()
  def yieldValue(m : Match) = {
    value
  }
}

