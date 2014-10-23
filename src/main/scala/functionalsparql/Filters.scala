package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.{Node, Triple}
import com.hp.hpl.jena.sparql.core._
import functionalsparql._


sealed trait Filter {
  def vars : Set[String]
  def canJoin(to : Filter) = (vars & to.vars).size > 0
  def join(to : Filter) : Filter 


}

sealed trait SparqlFilter extends Filter {
  def join(to : Filter) = to match {
    case sf : SparqlFilter => 
      JoinFilter((vars & to.vars).head, (vars & to.vars).tail, this, sf)
    case ef : ExpressionFilter => 
      JoinExprFilter(this, ef)
    case nf : NegativeFilter =>
      NegativeJoinFilter(vars & to.vars, this, nf)
  }
  def applyTo(rdd : DistCollection[Triple]) : DistCollection[Match]
  def optional = false
}

case class SimpleFilter(triple : Triple) extends SparqlFilter {
  lazy val vars = 
    extVar(triple.getSubject()) ++ 
    extVar(triple.getPredicate()) ++
    extVar(triple.getObject())

  private def extVar(n : Node) = n match {
    case v : Var => 
      Set(v.getVarName())
    case _ => 
      Set()
  }

  def applyTo(rdd : DistCollection[Triple]) = rdd.map { case t =>
    matchNode(t, triple.getSubject(), t.getSubject()) &
    matchNode(t, triple.getPredicate(), t.getPredicate()) &
    matchNode(t, triple.getObject(), t.getObject())
  } filter { m =>
    m != NoMatch
  }

  private def matchNode(t : Triple, n1 : Node, n2 : Node) : Match = 
    if(n1.isVariable()) {
      TripleMatch(List(t), Map(n1.asInstanceOf[Var].getVarName() -> n2))
    } else if(n1.isBlank() && !n2.isLiteral()) {
      TripleMatch(List(t), Map())
    } else if(n1 == n2) {
      TripleMatch(List(t), Map())
    } else {
      NoMatch
    }
}

case class JoinFilter[A](varName : String, varNames : Set[String], left : SparqlFilter, right : SparqlFilter) extends SparqlFilter {
  lazy val vars = left.vars ++ right.vars

  def applyTo(rdd : DistCollection[Triple]) = {
    val rdd1 = left.applyTo(rdd)
    val rdd3 = rdd1.keyFilter {
      case TripleMatch(triples, binding) => 
        if(binding.contains(varName)) {
          Some(binding(varName))
        } else {
          None
        }
      case NoMatch => 
        None
    }
    val rdd2 = right.applyTo(rdd)
    val rdd4 = rdd2.keyFilter {
      case TripleMatch(triples, binding) => 
        if(binding.contains(varName)) {
          Some(binding(varName))
        } else {
          None
        }
      case NoMatch => 
        None
    }
    val rdd5 = rdd3.join(rdd4).map {
      case (v,w) => v & w
    }
    val firstPass = left.optional match {
      case true =>
        right.optional match {
          case true =>
             rdd5 ++ rdd1 ++ rdd2
          case false =>
             rdd5 ++ rdd1
        }
      case false =>
        right.optional match {
          case true =>
            rdd5 ++ rdd2
          case false =>
            rdd5
        }
    }
    // TODO : Check reduce on other variables and unbound vars
    firstPass
  }
}

case class CrossFilter(filter1 : SparqlFilter, filter2 : SparqlFilter) extends SparqlFilter {
  lazy val vars = filter1.vars ++ filter2.vars

  def applyTo(rdd : DistCollection[Triple]) = filter1.applyTo(rdd).
    cartesian(filter2.applyTo(rdd)).
    map {
      case (m1,m2) => m1 x m2
    }
}

case class OptionalFilter(filter : SparqlFilter) extends SparqlFilter {
  def vars = filter.vars
  def applyTo(rdd : DistCollection[Triple]) = filter.applyTo(rdd)
  override def optional = true
}

case class UnionFilter(filters : Seq[SparqlFilter]) extends SparqlFilter {
  def vars = filters.flatMap(_.vars).toSet
  def applyTo(rdd : DistCollection[Triple]) = filters.map { filter =>
    filter.applyTo(rdd)
  } reduce {
    (x,y) => x ++ y
  }
}

case class NegativeFilter(filter : SparqlFilter) extends Filter {
  def vars = filter.vars
  def applyTo(rdd : DistCollection[Triple]) = filter.applyTo(rdd)
  override def canJoin(to : Filter) = to match {
    case _ : ExpressionFilter => false
    case _ => super.canJoin(to)
  }
  def join(to : Filter) = to match {
    case sq : SparqlFilter => NegativeJoinFilter(vars & to.vars, sq, this)
    case NegativeFilter(f2) => NegativeFilter(JoinFilter((vars & to.vars).head, (vars & to.vars).tail, filter, f2))
    case ef : ExpressionFilter => throw new IllegalArgumentException("Unreachable")
  }
}

case class NegativeJoinFilter(varNames : Set[String], posFilter : SparqlFilter, negFilter : NegativeFilter) extends SparqlFilter {
  def vars = posFilter.vars ++ negFilter.vars
  def applyTo(rdd : DistCollection[Triple]) = {
    val rdd1 = posFilter.applyTo(rdd)
    val rdd3 = rdd1.keyFilter {
      case TripleMatch(triples, binding) => 
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
      case TripleMatch(triples, binding) => 
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

case class UnboundNegativeFilter(filter : NegativeFilter) extends SparqlFilter {
  def vars = filter.vars
  def applyTo(rdd : DistCollection[Triple]) = throw new RuntimeException("Evaluating an unbound not exists is a very bad idea (if legal in SPARQL) you should reconsider your query!")
}

case class UnboundExprFilter(filter : ExpressionFilter) extends SparqlFilter {
  def vars = filter.vars
  def applyTo(rdd : DistCollection[Triple]) = {
    System.err.println("Evaluating an unbound filter is a very bad idea (if legal in SPARQL) you should reconsider your query!")
    val matches = rdd.map { t =>
      TripleMatch(Seq(t),Map()) : Match
    }
    filter.applyTo(matches)
  }
}

case class JoinExprFilter(filter1 : SparqlFilter, filter2 : ExpressionFilter) extends SparqlFilter {
  def vars = filter1.vars
  def applyTo(rdd : DistCollection[Triple]) = {
    filter2.applyTo(filter1.applyTo(rdd))
  }
}

object NullFilter extends SparqlFilter {
  def vars = Set()

  def applyTo(rdd : DistCollection[Triple]) = rdd.flatMap(t => None)
}

sealed trait ExpressionFilter extends Filter {
  override def canJoin(to : Filter) = to match {
    case _ : NegativeFilter => false
    case _ => super.canJoin(to)
  }
  def applyTo(rdd : DistCollection[Match]) : DistCollection[Match]
  def join(to : Filter) = to match {
    case sf : SparqlFilter => sf.join(this)
    case ef : ExpressionFilter => CatFilter(this,ef)
    case nf : NegativeFilter => throw new IllegalArgumentException()
  }
  def yieldValue(m : Match) : Any
}

case class ValueFilter(value : Any) extends ExpressionFilter {
  def vars = Set()
  def applyTo(rdd : DistCollection[Match]) = rdd.filter { m => FunctionalSparqlUtils.booleanFromAny(value) }
  def yieldValue(m : Match) = value
}

case class Expr0Filter(foo : Match => Any) extends ExpressionFilter {
  def vars = Set()
  def applyTo(rdd : DistCollection[Match]) = rdd.filter { m => FunctionalSparqlUtils.booleanFromAny(foo(m)) }
  def yieldValue(m : Match) = foo(m)
}

case class Expr0LogicalFilter(foo : Match => Boolean) extends ExpressionFilter {
  def vars = Set()
  def applyTo(rdd : DistCollection[Match]) = rdd.filter(foo)
  def yieldValue(m : Match) = foo(m)
}

case class Expr1Filter(expr1 : ExpressionFilter, foo : Any => Any) extends ExpressionFilter {
  def vars = expr1.vars
  def applyTo(rdd : DistCollection[Match]) = rdd.filter { m => FunctionalSparqlUtils.booleanFromAny(foo(expr1.yieldValue(m))) }
  def yieldValue(m : Match) = foo(expr1.yieldValue(m))
}

case class Expr1LogicalFilter(expr1 : ExpressionFilter, foo : Any => Boolean) extends ExpressionFilter {
  def vars = expr1.vars
  def applyTo(rdd : DistCollection[Match]) = rdd.filter(m => foo(expr1.yieldValue(m)))
  def yieldValue(m : Match) = foo(expr1.yieldValue(m))
}

case class Expr2Filter(expr1 : ExpressionFilter, expr2 : ExpressionFilter, foo : (Any, Any) => Any) extends ExpressionFilter {
  def vars = expr1.vars ++ expr2.vars
  def applyTo(rdd : DistCollection[Match]) = rdd.filter { m => FunctionalSparqlUtils.booleanFromAny(foo(expr1.yieldValue(m), expr2.yieldValue(m))) }
  def yieldValue(m : Match) = foo(expr1.yieldValue(m), expr2.yieldValue(m))
}

case class Expr2LogicalFilter(expr1 : ExpressionFilter, expr2 : ExpressionFilter, foo : (Any, Any) => Boolean) extends ExpressionFilter {
  def vars = expr1.vars ++ expr2.vars
  def applyTo(rdd : DistCollection[Match]) = rdd.filter(m => foo(expr1.yieldValue(m), expr2.yieldValue(m)))
  def yieldValue(m : Match) = foo(expr1.yieldValue(m), expr2.yieldValue(m))
}

case class Expr3Filter(expr1 : ExpressionFilter, expr2 : ExpressionFilter, expr3 : ExpressionFilter, foo : (Any, Any, Any) => Any) extends ExpressionFilter {
  def vars = expr1.vars ++ expr2.vars ++ expr3.vars
  def applyTo(rdd : DistCollection[Match]) = rdd.filter { m => FunctionalSparqlUtils.booleanFromAny(foo(expr1.yieldValue(m), expr2.yieldValue(m), expr3.yieldValue(m))) }
  def yieldValue(m : Match) = foo(expr1.yieldValue(m), expr2.yieldValue(m), expr3.yieldValue(m))
}

class Expr3LogicalFilter(expr1 : ExpressionFilter, expr2 : ExpressionFilter, expr3 : ExpressionFilter, foo : (Any, Any, Any) => Boolean) extends ExpressionFilter {
  def vars = expr1.vars ++ expr2.vars ++ expr3.vars
  def applyTo(rdd : DistCollection[Match]) = rdd.filter(m => foo(expr1.yieldValue(m), expr2.yieldValue(m), expr3.yieldValue(m)))
  def yieldValue(m : Match) = foo(expr1.yieldValue(m), expr2.yieldValue(m), expr3.yieldValue(m))
}

case class VariableExpressionFilter(variable : Var) extends ExpressionFilter {
  def vars = Set(variable.getVarName())
  def applyTo(rdd : DistCollection[Match]) = rdd.filter { m => FunctionalSparqlUtils.booleanFromAny(yieldValue(m)) }
  def yieldValue(m : Match) = m match {
    case TripleMatch(_, bindings) => if(bindings.contains(variable.getVarName())) {
      bindings(variable.getVarName())
    } else {
      variable
    }
    case _ => variable
  }
}

case class CatFilter(expr1 : ExpressionFilter, expr2 : ExpressionFilter) extends ExpressionFilter {
  def vars = expr1.vars ++ expr2.vars
  def applyTo(rdd : DistCollection[Match]) = expr2.applyTo(expr1.applyTo(rdd))
  def yieldValue(m : Match) = (expr1.yieldValue(m), expr2.yieldValue(m))
}

object NullExpressionFilter extends ExpressionFilter {
  def vars = Set()
  def applyTo(rdd : DistCollection[Match]) = throw new RuntimeException("Filter being applied to null value")
  def yieldValue(m : Match) = ""
}