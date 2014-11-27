package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.{Node, NodeFactory}
import com.hp.hpl.jena.sparql.core.Var
import org.joda.time.{DateTime, LocalDate, LocalTime}

class SparqlValueOrdering(dirs : Seq[Int]) extends Ordering[Seq[Any]] {
  import Expressions._
  def compare(x : Seq[Any], y : Seq[Any]) : Int = {
    require(x.size == y.size)
    require(x.size == dirs.size)
    (x zip y zip dirs).find { case ((a, b), d) =>
      d * doCompare(nodeFromAny(a), nodeFromAny(b)) != 0
    } map { case ((a, b), d) =>
      d * doCompare(nodeFromAny(a), nodeFromAny(b))
    } getOrElse (0)
  }
  def rdfTypeOrdering(x : Node) : Int = if(x.isVariable()) {
    0
  } else if(x.isBlank()) {
    1
  } else if(x.isURI()) {
    2
  } else /*if(x.isLiteral()) */ {
    3
  }
  def doCompare(x : Node, y : Node) : Int = {
    def lexicalComparison = x.toString.compareTo(x.toString) match {
      case 0 if !x.matches(y) => x.hashCode.compareTo(y.hashCode)
      case code => code
    }
    (rdfTypeOrdering(x) - rdfTypeOrdering(y)) match {
      case 0 =>
        if(x.isLiteral() && (x.getLiteralDatatypeURI() != null || y.getLiteralDatatypeURI() != null)) {
          anyFromNode(x) match {
            case i : Int =>
              anyFromNode(y) match {
                case i2 : Int => i.compareTo(i2)
                case f : Float => i.toFloat.compareTo(f)
                case d : Double => i.toDouble.compareTo(d)
                case bd : BigDecimal => BigDecimal(i).compare(bd)
                case _ => lexicalComparison
              }
            case f : Float =>
              anyFromNode(y) match {
                case i : Int => f.compareTo(i.toFloat)
                case f2 : Float => f.compareTo(f2)
                case d : Double => f.compareTo(d.toFloat)
                case bd : BigDecimal => f.compareTo(bd.toFloat)
                case _ => lexicalComparison
              }
            case d : Double =>
              anyFromNode(y) match {
                case i : Int => d.compareTo(i.toDouble)
                case f : Float => d.compareTo(f.toDouble)
                case d2 : Double => d.compareTo(d2)
                case bd : BigDecimal => d.compareTo(bd.toDouble)
                case _ => lexicalComparison
              }
            case bd : BigDecimal =>
              anyFromNode(y) match {
                case i : Int => bd.compare(BigDecimal(i))
                case f2 : Float => bd.compare(BigDecimal(f2))
                case d : Double => bd.compare(BigDecimal(d))
                case bd : BigDecimal => bd.compare(bd)
                case _ => lexicalComparison
              }
            case s : String => lexicalComparison
            case l : LangString => lexicalComparison
            case ul : UnsupportedLiteral => lexicalComparison
            case el : ErrorLiteral => lexicalComparison
            case d : DateTime => 
              anyFromNode(y) match {
                case d2 : DateTime => d.compareTo(d2)
                case _ => lexicalComparison
              }
            case pd : LocalDate =>
              anyFromNode(y) match {
                case d2 : LocalDate => pd.compareTo(d2)
                case _ => lexicalComparison
              }
            case l : Logic =>
              anyFromNode(y) match {
                case l2 : Logic => l.compareTo(l2)
                case _ => lexicalComparison
              }
          }
        } else {
          lexicalComparison
        }
      case other => 
        other
    }
  }
}
