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
    } else if(n1.isBlank() && !n2.isLiteral()) {
      Some(Match(Set(t), Map()))
    } else if(n1 == n2) {
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

case class LeftJoinFilter(left : Filter, right : Filter, cond : ExpressionFilter) extends Filter {
  def vars = left.vars
  val varNames = (left.vars & right.vars).toSeq

  def applyTo(rdd : DistCollection[Quad]) = {
    val rdd1 = left.applyTo(rdd)
    val rdd2 = right.applyTo(rdd)
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
    val rdd5 = rdd3.cogroup(rdd4)
    rdd5.flatMap { case (ls,rs) =>
      if(rs.isEmpty) {
        ls
      } else {
        ls.flatMap { l => 
          // Below is the real SPARQL logic
          //val join = rs.flatMap { r => 
          //  if(l.compatible(r)) {
          //    val lr = l & r
          //    if(cond.applyOnce(lr)) {
          //      Some(lr)
          //    } else {
          //      None
          //    }
          //  } else {
          //    None
          //  }
          //}
          //val diff = rs.forall { r =>
          //  !l.compatible(r) || !cond.applyOnce(l & r)
          //}
          //if(diff) {
          //  join ++ Set(l)
          //} else {
          //  join
          //}
          // This is slightly optimized
          rs.foldLeft((true, Seq[Match](), Seq[Match]())) { (acc,r) =>
            if(l.compatible(r)) {
              val lr = l & r
              if(cond.applyOnce(lr)) {
                (false, Nil, acc._3 :+ lr)
              } else if(acc._1) {
                (true, acc._2 :+ l, acc._3)
              } else {
                (false, Nil, acc._3)
              }
            } else if(acc._1) {
              (true, acc._2 :+ l, acc._3)
            } else {
              (false, Nil, acc._3)
            }
          } match {
            case (_, x, y) => y ++ x
          }
        }
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

/*case class GraphFilter(graphNode : Node, body : Filter) extends Filter {
  def vars = Filter.extVar(graphNode) ++ body.vars
  def applyTo(rdd : DistCollection[Quad]) = if(graphNode.isVariable()) {
    // Perhaps enable groupBy to speed this up??
    val x= body.applyTo(rdd).flatMap {
      case Match(triples, binding) => 
        val graphName = triples.headOption.map(_.getGraph()).getOrElse(null)
        if(graphName != null && triples.forall { t => t.getGraph() == graphName }) {
          val varName = graphNode.asInstanceOf[Var].getVarName()
          if(!binding.contains(varName) || binding(varName) == graphName) {
            Some(Match(triples, binding + (varName -> graphName)))
          } else {
            None
          }
        } else {
          None
        }
    }
    println(x)
    x
  } else {
    body.applyTo(rdd.filter { t => 
      t.getGraph() == graphNode
    })
  }
}*/


/*case class NegativeFilter(filter : Filter) {
  def applyTo(rdd : DistCollection[Quad]) = filter.applyTo(rdd)
}

case class NegativeJoinFilter(varNames : Set[String], posFilter : Filter, negFilter : NegativeFilter) extends Filter {
  def vars = posFilter.vars ++ negFilter.vars
  def applyTo(rdd : DistCollection[Quad]) = {
    val rdd1 = posFilter.applyTo(rdd)
    val rdd3 = rdd1.keyFilter {
      case Match(triples, binding) => 
        if(varNames.forall(varName => binding.contains(varName))) {
          Some(varNames.toSeq.map(varName => binding(varName)))
        } else {
          None
        }
      case NoMatch => 
        None
    }
    val rdd2 = negFilter.applyTo(rdd)
    val rdd4 = rdd2.keyFilter {
      case Match(triples, binding) => 
        if(varNames.forall(varName => binding.contains(varName))) {
          Some(varNames.toSeq.map(varName => binding(varName)))
        } else {
          None
        }
      case NoMatch => 
        None
    }
    rdd3.cogroup(rdd4).flatMap {
      case (i1,i2) => if(i1.iterator.hasNext && !i2.iterator.hasNext) {
        i1
      } else {
        Nil
      }
    }
  }
}

case class UnboundNegativeFilter(filter : NegativeFilter) extends Filter {
  def vars = filter.vars
  def applyTo(rdd : DistCollection[Quad]) = throw new RuntimeException("Evaluating an unbound not exists is a very bad idea (if legal in SPARQL) you should reconsider your query!")
}*/

case class FilterFilter(filter2 : ExpressionFilter, filter1 : Filter) extends Filter {
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

sealed trait ExpressionFilter {
  def vars : Set[String]
  def applyOnce(m : Match) : Boolean
  def applyTo(rdd : DistCollection[Match]) = rdd.filter(applyOnce)
  def yieldValue(m : Match) : Any
}

case class ValueFilter(value : Any) extends ExpressionFilter {
  def vars = Set()
  def applyOnce(m : Match) = FunctionalSparqlUtils.booleanFromAny(value)
  def yieldValue(m : Match) = value
}

case class Expr0Filter(foo : Match => Any) extends ExpressionFilter {
  def vars = Set()
  def applyOnce(m : Match) = FunctionalSparqlUtils.booleanFromAny(foo(m))
  def yieldValue(m : Match) = foo(m)
}

case class Expr0LogicalFilter(foo : Match => Boolean) extends ExpressionFilter {
  def vars = Set()
  def applyOnce(m : Match) = foo(m)
  def yieldValue(m : Match) = foo(m)
}

case class Expr1Filter(expr1 : ExpressionFilter, foo : Any => Any) extends ExpressionFilter {
  def vars = expr1.vars
  def applyOnce(m : Match) = FunctionalSparqlUtils.booleanFromAny(foo(expr1.yieldValue(m))) 
  def yieldValue(m : Match) = foo(expr1.yieldValue(m))
}

case class Expr1LogicalFilter(expr1 : ExpressionFilter, foo : Any => Boolean) extends ExpressionFilter {
  def vars = expr1.vars
  def applyOnce(m : Match) = foo(expr1.yieldValue(m))
  def yieldValue(m : Match) = foo(expr1.yieldValue(m))
}

case class Expr2Filter(expr1 : ExpressionFilter, expr2 : ExpressionFilter, foo : (Any, Any) => Any) extends ExpressionFilter {
  def vars = expr1.vars ++ expr2.vars
  def applyOnce(m : Match) = FunctionalSparqlUtils.booleanFromAny(foo(expr1.yieldValue(m), expr2.yieldValue(m))) 
  def yieldValue(m : Match) = foo(expr1.yieldValue(m), expr2.yieldValue(m))
}

case class Expr2LogicalFilter(expr1 : ExpressionFilter, expr2 : ExpressionFilter, foo : (Any, Any) => Boolean) extends ExpressionFilter {
  def vars = expr1.vars ++ expr2.vars
  def applyOnce(m : Match) = {
    //val x1 = expr1.yieldValue(m)
    //val x2 = expr2.yieldValue(m)
    //println("%s op %s = %s" format (x1.toString, x2.toString, foo(x1,x2).toString))
    foo(expr1.yieldValue(m), expr2.yieldValue(m))
  }
  def yieldValue(m : Match) = foo(expr1.yieldValue(m), expr2.yieldValue(m))
}

case class Expr3Filter(expr1 : ExpressionFilter, expr2 : ExpressionFilter, expr3 : ExpressionFilter, foo : (Any, Any, Any) => Any) extends ExpressionFilter {
  def vars = expr1.vars ++ expr2.vars ++ expr3.vars
  def applyOnce(m : Match) = FunctionalSparqlUtils.booleanFromAny(foo(expr1.yieldValue(m), expr2.yieldValue(m), expr3.yieldValue(m))) 
  def yieldValue(m : Match) = foo(expr1.yieldValue(m), expr2.yieldValue(m), expr3.yieldValue(m))
}

class Expr3LogicalFilter(expr1 : ExpressionFilter, expr2 : ExpressionFilter, expr3 : ExpressionFilter, foo : (Any, Any, Any) => Boolean) extends ExpressionFilter {
  def vars = expr1.vars ++ expr2.vars ++ expr3.vars
  def applyOnce(m : Match) = foo(expr1.yieldValue(m), expr2.yieldValue(m), expr3.yieldValue(m))
  def yieldValue(m : Match) = foo(expr1.yieldValue(m), expr2.yieldValue(m), expr3.yieldValue(m))
}

case class VariableExpressionFilter(variable : Var) extends ExpressionFilter {
  def vars = Set(variable.getVarName())
  def applyOnce(m : Match) = FunctionalSparqlUtils.booleanFromAny(yieldValue(m)) 
  def yieldValue(m : Match) = m match {
    case Match(_, bindings) => if(bindings.contains(variable.getVarName())) {
      bindings(variable.getVarName())
    } else {
      println("Could not bind %s in %s" format (variable.getVarName(), bindings.toString))
      variable
    }
    case _ => throw new SparqlEvaluationException("Looking up variable against no match")
  }
}

case class ConjFilter(expr1 : ExpressionFilter, expr2 : ExpressionFilter) extends ExpressionFilter {
  def vars = expr1.vars ++ expr2.vars
  def applyOnce(m : Match) = expr1.applyOnce(m) && expr2.applyOnce(m)
  def yieldValue(m : Match) = (expr1.yieldValue(m), expr2.yieldValue(m))
}

object TrueFilter extends ExpressionFilter {
  def vars = Set()
  def applyOnce(m : Match) = true
  override def applyTo(rdd : DistCollection[Match]) = rdd
  def yieldValue(m : Match) = true
}
