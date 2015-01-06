package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.sparql.core._
import functionalsparql._

sealed trait Filter {
  def vars : Set[String]
  def applyTo(rdd : DistCollection[Quad]) : DistCollection[Match]
  def withGraph(graph : Node) : Filter
}

object Filter {
  def extVar(n : Node) = n match {
    case v : Var => 
      Set(v.getVarName())
    case _ => 
      Set()
  }
}

case class SimpleFilter(triple : Quad) extends Filter {
  import Filter.extVar
  lazy val vars = 
    extVar(triple.getGraph()) ++ 
    extVar(triple.getSubject()) ++ 
    extVar(triple.getPredicate()) ++
    extVar(triple.getObject())

    def applyTo(rdd : DistCollection[Quad]) = rdd.flatMap { case t =>
    matchNode(t, triple.getSubject(), t.getSubject()) match {
      case Some(sm) => matchNode(t, triple.getPredicate(), t.getPredicate()) match {
        case Some(pm) => matchNode(t, triple.getObject(), t.getObject()) match {
          case Some(om) => matchGraphNode(t, triple.getGraph(), t.getGraph()) match {
            case Some(gm) => if(sm.compatible(pm)) {
              val spm = sm & pm
              if(spm.compatible(om)) {
                val spom = spm & om
                if(spom.compatible(gm)) {
                  Some(spom & gm)
                } else {
                  None
                }
              } else {
                None
              }
            } else {
              None
            }
            case None => None
          }
          case None => None
        }
        case None => None
      }
      case None => None
    }
  } 

  private def matchGraphNode(t : Quad, n1 : Node, n2 : Node) : Option[Match] = {
    if(n1.isVariable() && n2 == Quad.defaultGraphIRI) { 
      return None
    } else {
      return matchNode(t, n1, n2)
    }
  }

  private def matchNode(t : Quad, n1 : Node, n2 : Node) : Option[Match] = {
    if(n1.isVariable()) {
      Some(Match(Set(t), Map(n1.asInstanceOf[Var].getVarName() -> n2)))
    } else if(n1.isBlank() && !n2.isLiteral() && !n2.isBlank()) {
      Some(Match(Set(t), Map()))
    } else if(n1 == n2) {
      Some(Match(Set(t), Map()))
    } else if(n1.isLiteral() && n2.isLiteral() && 
        n1.getLiteralLexicalForm() == n2.getLiteralLexicalForm() &&
        n1.getLiteralDatatype() == n2.getLiteralDatatype() &&
        (n1.getLiteralLanguage() == null ||  
        n2.getLiteralLanguage() == null || 
        n1.getLiteralLanguage().toLowerCase == 
          n2.getLiteralLanguage().toLowerCase)) {
      Some(Match(Set(t), Map()))
    } else if(n2.isVariable()) {
      throw new IllegalArgumentException("Variable in data")
    } else {
      None
    }
  }

  def withGraph(graph : Node) = SimpleFilter(new Quad(graph, triple.asTriple()))
}

case class JoinFilter(left : Filter, right : Filter) extends Filter {
  lazy val vars = left.vars ++ right.vars
  val varNames = (left.vars & right.vars).toSeq

  def applyTo(rdd : DistCollection[Quad]) = {
    val rdd1 = left.applyTo(rdd)
    val rdd2 = right.applyTo(rdd)
    val rdd5 = if(varNames.isEmpty) {
      rdd1.cartesian(rdd2)
    } else {
      val rdd3 = rdd1.keyFilter {
        case Match(triples, binding) => 
          Some(varNames.map { varName =>
            binding.get(varName).getOrElse(throw new RuntimeException())
          })
      }
      val rdd4 = rdd2.keyFilter {
        case Match(triples, binding) => 
          Some(varNames.map { varName =>
            binding.get(varName).getOrElse(throw new RuntimeException())
          })
      }
      rdd3.join(rdd4)
    }
    rdd5.flatMap { case (l,r) =>
      if(l.compatible(r)) {
        Some(l & r)
      } else {
        None
      }
    }
  }
  
  def withGraph(graph : Node) = JoinFilter(left.withGraph(graph), right.withGraph(graph))
}

case class LeftJoinFilter(left : Filter, right : Filter, cond : Expression) extends Filter {
  def vars = left.vars
  val varNames = (left.vars & right.vars).toSeq

  def applyTo(rdd : DistCollection[Quad]) = {
    val rdd1 = left.applyTo(rdd)
    val rdd2 = right.applyTo(rdd)
    if(varNames.isEmpty) {
      rdd1.flatMap { l =>
        if(!cond.applyOnce(l) && cond.vars.forall(left.vars.contains(_))) {
          Some(l)
        } else {
          (rdd2.flatMap { r =>
            if(l.compatible(r)) {
              val lr = l & r
              if(cond.applyOnce(lr)) {
                Some(lr)
              } else {
                Some(l)
              }
            } else {
              Some(l)
            }
          }).toIterable
        }
      }
    } else {
      val rdd3 = rdd1.keyFilter {
        case Match(triples, binding) => 
          Some(varNames.map { varName =>
            binding.get(varName).getOrElse(throw new RuntimeException())
          })
      }
      val rdd4 = rdd2.keyFilter {
        case Match(triples, binding) => 
          Some(varNames.map { varName =>
            binding.get(varName).getOrElse(throw new RuntimeException())
          })
      }
      val rdd5 = rdd3.leftJoin(rdd4)
      rdd5.flatMap {
        case (l, Some(r)) =>
          if(l.compatible(r)) {
            val lr = l & r
            if(cond.applyOnce(lr)) {
              Some(lr)
            } else {
              Some(l)
            }
          } else {
            Some(l)
          }
        case (l, None) =>
          Some(l)
      }
    }
  }

  def withGraph(graph : Node) = LeftJoinFilter(left.withGraph(graph), right.withGraph(graph), cond)
}

case class UnionFilter(filters : Seq[Filter]) extends Filter {
  //def vars = filters.flatMap(_.vars).toSet // Perhaps this should be the intersection of all??
  def vars = filters.map(_.vars).reduce { (x,y) =>
    x & y
  }
  def applyTo(rdd : DistCollection[Quad]) = filters.map { filter =>
    filter.applyTo(rdd)
  } reduce {
    (x,y) => x ++ y
  }

  def withGraph(graph : Node) = UnionFilter(filters.map(_.withGraph(graph)))
}

case class FilterFilter(filter2 : Expression, filter1 : Filter) extends Filter {
  def vars = filter1.vars ++ filter2.vars
  def applyTo(rdd : DistCollection[Quad]) = {
    filter2.applyTo(filter1.applyTo(rdd))
  }
  def withGraph(graph : Node) = FilterFilter(filter2, filter1.withGraph(graph))
}

object NullFilter extends Filter {
  def vars = Set()
  def applyTo(rdd : DistCollection[Quad]) = rdd.map { t => 
    Match(Set(t),Map())
  }
  def withGraph(graph : Node) = NullFilter
}
