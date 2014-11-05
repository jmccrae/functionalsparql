package eu.liderproject.functionalsparql

import com.hp.hpl.jena.graph.{Node, NodeFactory, Triple}
import com.hp.hpl.jena.query.{Query, QueryFactory}
import com.hp.hpl.jena.sparql.core._
import com.hp.hpl.jena.sparql.expr._
import com.hp.hpl.jena.sparql.expr.nodevalue._
import com.hp.hpl.jena.sparql.syntax._
import java.io.StringReader
import java.net.URI
import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.annotation.tailrec
import scala.collection.JavaConversions._

object functionalsparql {
  import FunctionalSparqlUtils._
  implicit val nodeOrdering = new Ordering[Node] {
    override def compare(n1 : Node, n2 : Node) = {
      if(n1 == null) {
        if(n2 == null) {
          0
        } else {
          -1 
        }
      } else if(n2 == null) {
        +1
      } else {
        n1.toString.compareTo(n2.toString)
      }
    }
  }
  implicit val nodeSeqOrdering = new Ordering[Seq[Node]] {
    override def compare(n1 : Seq[Node], n2 : Seq[Node]) : Int = n1.headOption match {
      case Some(e1) => n2.headOption match {
        case Some(e2) => nodeOrdering.compare(e1,e2) match {
          case 0 => compare(n1.tail,n2.tail)
          case nonZero => nonZero
        }
        case None => +1
      }
      case None =>  n2.headOption match {
        case Some(_) => -1
        case None => 0
      }
    }
  }

  def parseRDD(rdd : DistCollection[String], base : String) : DistCollection[Quad] = {
    rdd.flatMap { 
      line =>
        val collector = new StreamRDFCollector()
        RDFDataMgr.parse(collector, new StringReader(line), base, Lang.NTRIPLES)
        collector.triples
    }
  }
  
  def parseQuery(query : String) : Query = QueryFactory.create(query)

  def processQuery(query : String) : Plan[_] = processQuery(parseQuery(query))

  def processQuery(query : Query) : Plan [_]= {
    query.getQueryType() match {
      case Query.QueryTypeAsk => AskPlan(processElement(query.getQueryPattern()))
      case Query.QueryTypeConstruct => ConstructPlan(processTemplate(query.getConstructTemplate()), processElement(query.getQueryPattern()))
      case Query.QueryTypeDescribe => DescribePlan(query.getResultURIs(),processElement(query.getQueryPattern()))
      case Query.QueryTypeSelect => SelectPlan(query.getResultVars().toList,
        processElement(query.getQueryPattern()), query.isDistinct())
      case _ => throw new UnsupportedOperationException("Unknown SPARQL query type")
    }
  }

  private[functionalsparql] def processTemplate(template : Template) = template.getBGP()

  private[functionalsparql] def processElement(element : Element) : Filter = element match {
    case element : ElementExists => throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
    case element : ElementNotExists => throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
    case element : ElementAssign => throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
    case element : ElementData => throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
    case element : ElementDataset => throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
    case element : ElementFilter => transform(element)
    case element : ElementGroup => transform(element)
    case element : ElementMinus => throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
    case element : ElementNamedGraph => transform(element)
    case element : ElementOptional => transform(element)
    case element : ElementPathBlock => transform(element)
    case element : ElementService => throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
    case element : ElementSubQuery => throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
    case element : ElementTriplesBlock => transform(element)
    case element : ElementUnion => transform(element)
    case _ => throw new UnsupportedOperationException("Unknown Element type %s" format (element.getClass().getName()))
  }

  private [functionalsparql] def isSparql(filter : Filter) = filter match {
    case sf : Filter => Some(sf)
    case _ => None
  }

  private [functionalsparql] def transform(element : Element) : Filter = element match {
    case e : ElementTriplesBlock => processBasicPattern(e.getPattern())
    case e : ElementPathBlock => plan(processPathBlock(e.getPattern()))
    case e : ElementUnion => e.getElements.map(processElement).foldLeft(UnionFilter(Nil)) { case (UnionFilter(fs),f) =>
      UnionFilter(f +: fs)
    }
    case e : ElementNamedGraph => {
      processElement(e.getElement()).withGraph(e.getGraphNameNode())
      //GraphFilter(e.getGraphNameNode(), processElement(e.getElement()))
    }
    case e : ElementGroup => {
      var fs : ExpressionFilter = TrueFilter
      var g : Filter = NullFilter
      for(e2 <- e.getElements()) {
        e2 match {
          case e3 : ElementFilter => 
            if(fs == TrueFilter) {
              fs = processExpression(e3.getExpr())
            } else {
              fs = ConjFilter(fs, processExpression(e3.getExpr()))
            }
          case e3 : ElementOptional => 
            val a = transform(e3.getOptionalElement())
            a match {
              case FilterFilter(f, a2) =>
                g = LeftJoinFilter(g, a2, f)
              case _ =>
                g = LeftJoinFilter(g, a, TrueFilter)
            }
          case _ => 
            val a = transform(e2)
            if(g == NullFilter) {
              g = a
            } else {
              g = JoinFilter(g, a)
            }
        }
      }
      g match {
        case JoinFilter(NullFilter,g2) => 
          g = g2
        case JoinFilter(g2, NullFilter) =>
          g = g2
        case _ => {}
      }
      if(fs != TrueFilter) {
        FilterFilter(fs, g)
      } else {
        g
      }
    }
  }

  private [functionalsparql] def processBasicPattern(pattern : BasicPattern) = 
    plan(pattern.map(t => SimpleFilter(new Quad(Quad.defaultGraphIRI, t))).toList)
  private [functionalsparql] def processPathBlock(pathBlock : PathBlock) =
    pathBlock.map(processTriplePath(_)).toList
  private [functionalsparql] def processTriplePath(triplePath : TriplePath) =
    if(triplePath.isTriple()) {
      SimpleFilter(new Quad(Quad.defaultGraphIRI, triplePath.asTriple()))
    } else {
      throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
    }

  def plan(triples : List[Filter]) : Filter = triples match {
    case Nil => NullFilter
    case x :: Nil => x
    case x :: xs => xs.find(y => !(x.vars & y.vars).isEmpty) match {
      case Some(y) => plan(JoinFilter(x,y) :: xs.filter(_ != y))
      case None => JoinFilter(x,plan(xs))
    }
  }

  private [functionalsparql] def processExpression(expr : Expr) : ExpressionFilter = expr match {
    case e : E_Add => processE_Add(e)
    case e : E_BNode => processE_BNode(e)
    case e : E_Bound => processE_Bound(e)  
    case e : E_Call => processE_Call(e)  
    case e : E_Cast => processE_Cast(e)  
    case e : E_Coalesce => processE_Coalesce(e)  
    case e : E_Conditional => processE_Conditional(e)  
    case e : E_Datatype => processE_Datatype(e)  
    case e : E_DateTimeDay => processE_DateTimeDay(e) 
    case e : E_DateTimeHours => processE_DateTimeHours(e)  
    case e : E_DateTimeMinutes => processE_DateTimeMinutes(e)  
    case e : E_DateTimeMonth => processE_DateTimeMonth(e)  
    case e : E_DateTimeSeconds => processE_DateTimeSeconds(e)  
    case e : E_DateTimeTimezone => processE_DateTimeTimezone(e)  
    case e : E_DateTimeTZ => processE_DateTimeTZ(e)  
    case e : E_DateTimeYear => processE_DateTimeYear(e) 
    case e : E_Divide => processE_Divide(e)  
    case e : E_Equals => processE_Equals(e)  
    case e : E_Exists => processE_Exists(e)  
    case e : E_Function => processE_Function(e)  
    //case e : E_FunctionDynamic => processE_FunctionDynamic(e)  
    case e : E_GreaterThan => processE_GreaterThan(e)  
    case e : E_GreaterThanOrEqual => processE_GreaterThanOrEqual(e)  
    case e : E_IRI => processE_IRI(e)  
    case e : E_IsBlank => processE_IsBlank(e)  
    case e : E_IsIRI => processE_IsIRI(e)  
    case e : E_IsLiteral => processE_IsLiteral(e)  
    case e : E_IsNumeric => processE_IsNumeric(e)  
    //case e : E_IsURI => processE_IsURI(e)  
    case e : E_Lang => processE_Lang(e)  
    case e : E_LangMatches => processE_LangMatches(e)  
    case e : E_LessThan => processE_LessThan(e)  
    case e : E_LessThanOrEqual => processE_LessThanOrEqual(e)  
    case e : E_LogicalAnd => processE_LogicalAnd(e)  
    case e : E_LogicalNot => processE_LogicalNot(e)  
    case e : E_LogicalOr => processE_LogicalOr(e)  
    case e : E_MD5 => processE_MD5(e)  
    case e : E_Multiply => processE_Multiply(e)  
    case e : E_NotEquals => processE_NotEquals(e)  
    case e : E_NotExists => processE_NotExists(e)  
    case e : E_NotOneOf => processE_NotOneOf(e)  
    case e : E_Now => processE_Now(e)  
    case e : E_NumAbs => processE_NumAbs(e)  
    case e : E_NumCeiling => processE_NumCeiling(e)  
    case e : E_NumFloor => processE_NumFloor(e)  
    case e : E_NumRound => processE_NumRound(e)  
    case e : E_OneOf => processE_OneOf(e)  
    case e : E_OneOfBase => processE_OneOfBase(e)  
    case e : E_Random => processE_Random(e)  
    case e : E_Regex => processE_Regex(e)  
    case e : E_SameTerm => processE_SameTerm(e)  
    case e : E_SHA1 => processE_SHA1(e)  
    case e : E_SHA224 => processE_SHA224(e)  
    case e : E_SHA256 => processE_SHA256(e)  
    case e : E_SHA384 => processE_SHA384(e)  
    case e : E_SHA512 => processE_SHA512(e)  
    case e : E_Str => processE_Str(e)  
    case e : E_StrAfter => processE_StrAfter(e)  
    case e : E_StrBefore => processE_StrBefore(e)  
    case e : E_StrConcat => processE_StrConcat(e)  
    case e : E_StrContains => processE_StrContains(e)  
    case e : E_StrDatatype => processE_StrDatatype(e)  
    case e : E_StrEncodeForURI => processE_StrEncodeForURI(e)  
    case e : E_StrEndsWith => processE_StrEndsWith(e)  
    case e : E_StrLang => processE_StrLang(e)  
    case e : E_StrLength => processE_StrLength(e)  
    case e : E_StrLowerCase => processE_StrLowerCase(e)  
    case e : E_StrReplace => processE_StrReplace(e)  
    case e : E_StrStartsWith => processE_StrStartsWith(e)  
    case e : E_StrSubstring => processE_StrSubstring(e)  
    case e : E_StrUpperCase => processE_StrUpperCase(e)  
    case e : E_StrUUID => processE_StrUUID(e)  
    case e : E_Subtract => processE_Subtract(e)  
    case e : E_UnaryMinus => processE_UnaryMinus(e)  
    case e : E_UnaryPlus => processE_UnaryPlus(e)  
    //case e : E_URI => processE_URI(e)  
    case e : E_UUID => processE_UUID(e)  
    case e : E_Version => processE_Version(e)  
    case e : ExprAggregator => processExprAggregator(e)  
    case e : ExprDigest => processExprDigest(e)  
    //case e : ExprNode => processExprNode(e)  
    //case e : ExprSystem => processExprSystem(e)  
    case e : ExprVar => processExprVar(e)  
    case e : NodeValueBoolean => processNodeValueBoolean(e)  
    case e : NodeValueDecimal => processNodeValueDecimal(e)  
    case e : NodeValueDouble => processNodeValueDouble(e)  
    case e : NodeValueDT => processNodeValueDT(e)  
    case e : NodeValueDuration => processNodeValueDuration(e)  
    case e : NodeValueFloat => processNodeValueFloat(e)  
    case e : NodeValueInteger => processNodeValueInteger(e)  
    case e : NodeValueNode => processNodeValueNode(e)  
    case e : NodeValueString => processNodeValueString(e) 
    case null => ValueFilter("")
    case _ => throw new IllegalArgumentException("Bad expression " + expr)
  }
    
  private def boolean1(e : ExprFunction1, foo : Boolean => Boolean) = {
    Expr1LogicalFilter(processExpression(e.getArg()), x => foo(booleanFromAny(x)))
  }

  private def numeric1(e : ExprFunction1, fooi : Int => Int, food : Double => Double, foof : Float => Float, foobi : BigInt => BigInt, foobd : BigDecimal => BigDecimal) = {
    def applyFoo(x : Any) : Any = x match {
      case n1 : Int => fooi(n1)
      case n1 : Double => food(n1)
      case n1 : Float => foof(n1)
      case n1 : BigInt => foobi(n1)
      case n1 : BigDecimal => foobd(n1)
      case n1 : Node => applyFoo(anyFromNode(n1))
      case _ => throw new SparqlEvaluationException("Arguments were not numeric")
    }
    Expr1Filter(processExpression(e.getArg()), applyFoo)
  }

  private def numeric2(e : ExprFunction2, fooi : (Int,Int) => Int, food : (Double,Double) => Double, 
    foof : (Float,Float) => Float, foobi : (BigInt,BigInt) => BigInt, foobd : (BigDecimal,BigDecimal) => BigDecimal) = {
    def applyFoo(x : Any, y : Any) : Any = x match {
        case n1 : Int => y match {
          case n2 : Int => fooi(n1,n2)
          case n2 : Double => food(n1.toDouble,n2)
          case n2 : Float => foof(n1.toFloat,n2)
          case n2 : BigInt => foobi(BigInt(n1),n2)
          case n2 : BigDecimal => foobd(BigDecimal(n1),n2)
          case n2 : Node => applyFoo(n1, anyFromNode(n2))
          case _ => throw new SparqlEvaluationException("Arguments were not numeric")
        }
        case n1 : Double => y match {
          case n2 : Int => food(n1,n2.toDouble)
          case n2 : Double => food(n1,n2)
          case n2 : Float => food(n1,n2.toDouble)
          case n2 : BigInt => foobd(BigDecimal(n1), BigDecimal(n2))
          case n2 : BigDecimal => foobd(BigDecimal(n1), n2)
          case n2 : Node => applyFoo(n1, anyFromNode(n2))
          case _ => throw new SparqlEvaluationException("Arguments were not numeric")
        }
        case n1 : Float => y match {
          case n2 : Int => food(n1.toDouble,n2)
          case n2 : Double => food(n1,n2)
          case n2 : Float => foof(n1,n2)
          case n2 : BigInt => foobd(BigDecimal(n1), BigDecimal(n2))
          case n2 : BigDecimal => foobd(BigDecimal(n1), n2)
          case n2 : Node => applyFoo(n1, anyFromNode(n2))
          case _ => throw new SparqlEvaluationException("Arguments were not numeric")
        } 
        case n1 : BigInt => y match {
          case n2 : Int => foobi(n1,BigInt(n2))
          case n2 : Double => foobd(BigDecimal(n1),BigDecimal(n2))
          case n2 : Float => foobd(BigDecimal(n1),BigDecimal(n2))
          case n2 : BigInt => foobi(n1,n2)
          case n2 : BigDecimal => foobd(BigDecimal(n1),n2)
          case n2 : Node => applyFoo(n1, anyFromNode(n2))
          case _ => throw new SparqlEvaluationException("Arguments were not numeric")
        }
        case n1 : BigDecimal => y match {
          case n2 : Int => foobd(n1,BigDecimal(n2))
          case n2 : Double => foobd(n1,BigDecimal(n2))
          case n2 : Float => foobd(n1,BigDecimal(n2))
          case n2 : BigInt => foobd(n1,BigDecimal(n2))
          case n2 : BigDecimal => foobd(n1,n2)
          case n2 : Node => applyFoo(n1, anyFromNode(n2))
          case _ => throw new SparqlEvaluationException("Arguments were not numeric")
        }
        case n1 : Node => applyFoo(anyFromNode(n1),y)
        case _ => throw new SparqlEvaluationException("Arguments were not numeric: " + x.getClass().getName())
      }
    Expr2Filter(processExpression(e.getArg1()), processExpression(e.getArg2()), applyFoo)
  }

  private def numericFromNode(n : Node) : Option[BigDecimal] = if(n.isLiteral()) {
    toNumOption(n.getLiteralValue().toString)
  } else {
    None
  }

  private def toNumOption(s : String) : Option[BigDecimal]  = try {
    Some(BigDecimal(s))
  } catch {
    case x : NumberFormatException => None
  }

  private def order2(e : ExprFunction2, fooi : (Int,Int) => Boolean, food : (Double,Double) => Boolean, 
    foof : (Float,Float) => Boolean, foobi : (BigInt,BigInt) => Boolean, foobd : (BigDecimal,BigDecimal) => Boolean,
    foos : (String,String) => Boolean, foon : (Node,Node) => Boolean) = {
    def applyFoo(x : Any, y : Any) : Boolean = x match {
        case n1 : Int => y match {
          case n2 : Int => fooi(n1,n2)
          case n2 : Double => food(n1.toDouble,n2)
          case n2 : Float => foof(n1.toFloat,n2)
          case n2 : BigInt => foobi(BigInt(n1),n2)
          case n2 : BigDecimal => foobd(BigDecimal(n1),n2)
          case n2 : String => toNumOption(n2) match {
            case Some(d) => foobd(BigDecimal(n1),d)
            case None => throw new SparqlEvaluationException("Cannot coerce string into numeric")
          }
          case n2 : Node => numericFromNode(n2) match {
            case Some(d) => foobd(BigDecimal(n1),d)
            case None => throw new SparqlEvaluationException("Cannot coerce string into numeric") 
          }
          case _ => throw new SparqlEvaluationException("Arguments were not orderable")
        }
        case n1 : Double => y match {
          case n2 : Int => food(n1,n2.toDouble)
          case n2 : Double => food(n1,n2)
          case n2 : Float => food(n1,n2.toDouble)
          case n2 : BigInt => foobd(BigDecimal(n1), BigDecimal(n2))
          case n2 : BigDecimal => foobd(BigDecimal(n1), n2)
          case n2 : String => toNumOption(n2) match {
            case Some(d) => foobd(BigDecimal(n1),d)
            case None => throw new SparqlEvaluationException("Cannot coerce string into numeric")
          }
          case n2 : Node => numericFromNode(n2) match {
            case Some(d) => foobd(BigDecimal(n1),d)
            case None => throw new SparqlEvaluationException("Cannot coerce string into numeric") 
          }
          case _ => throw new SparqlEvaluationException("Arguments were not orderable")
        }
        case n1 : Float => y match {
          case n2 : Int => food(n1.toDouble,n2)
          case n2 : Double => food(n1,n2)
          case n2 : Float => foof(n1,n2)
          case n2 : BigInt => foobd(BigDecimal(n1), BigDecimal(n2))
          case n2 : BigDecimal => foobd(BigDecimal(n1), n2)
          case n2 : String => toNumOption(n2) match {
            case Some(d) => foobd(BigDecimal(n1),d)
            case None => throw new SparqlEvaluationException("Cannot coerce string into numeric")
          }
          case n2 : Node => numericFromNode(n2) match {
            case Some(d) => foobd(BigDecimal(n1),d)
            case None => throw new SparqlEvaluationException("Cannot coerce string into numeric") 
          }
          case _ => throw new SparqlEvaluationException("Arguments were not orderable")
        } 
        case n1 : BigInt => y match {
          case n2 : Int => foobi(n1,BigInt(n2))
          case n2 : Double => foobd(BigDecimal(n1),BigDecimal(n2))
          case n2 : Float => foobd(BigDecimal(n1),BigDecimal(n2))
          case n2 : BigInt => foobi(n1,n2)
          case n2 : BigDecimal => foobd(BigDecimal(n1),n2)
          case n2 : String => toNumOption(n2) match {
            case Some(d) => foobd(BigDecimal(n1),d)
            case None => throw new SparqlEvaluationException("Cannot coerce string into numeric")
          }
          case n2 : Node => numericFromNode(n2) match {
            case Some(d) => foobd(BigDecimal(n1),d)
            case None => throw new SparqlEvaluationException("Cannot coerce string into numeric") 
          }
          case _ => throw new SparqlEvaluationException("Arguments were not orderable")
        }
        case n1 : BigDecimal => y match {
          case n2 : Int => foobd(n1,BigDecimal(n2))
          case n2 : Double => foobd(n1,BigDecimal(n2))
          case n2 : Float => foobd(n1,BigDecimal(n2))
          case n2 : BigInt => foobd(n1,BigDecimal(n2))
          case n2 : BigDecimal => foobd(n1,n2)
          case n2 : String => toNumOption(n2) match {
            case Some(d) => foobd(n1,d)
            case None => throw new SparqlEvaluationException("Cannot coerce string into numeric")
          }
          case n2 : Node => numericFromNode(n2) match {
            case Some(d) => foobd(n1,d)
            case None => throw new SparqlEvaluationException("Cannot coerce string into numeric") 
          }
          case _ => throw new SparqlEvaluationException("Arguments were not orderable")
        }
        case n1 : String => y match {
          case n2 : String => foos(n1,n2)
          case n2 : Node => foos(n1,stringFromAny(n2))
          case _ => toNumOption(n1) match {
            case Some(d) => y match {
              case n2 : Int => foobd(d,BigDecimal(n2))
              case n2 : Double => foobd(d,BigDecimal(n2))
              case n2 : Float => foobd(d,BigDecimal(n2))
              case n2 : BigInt => foobd(d,BigDecimal(n2))
              case n2 : BigDecimal => foobd(d,n2)
              case n2 : String => toNumOption(n2) match {
                case Some(d2) => foobd(d,d2)
                case None => foos(n1, stringFromAny(y))
              }
              case n2 : Node => numericFromNode(n2) match {
                case Some(d2) => foobd(d,d2)
                case None => foos(n1, stringFromAny(y))
              }
              case _ => throw new SparqlEvaluationException("Arguments were not orderable")
            }
            case None => foos(n1, stringFromAny(y))
          }
        }
        case n1 : Node => y match {
          case n2 : String => foos(stringFromAny(n1),n2)
          case n2 : Node => foon(n1,n2)
          case _ => toNumOption(stringFromAny(n1)) match {
            case Some(d) => y match {
              case n2 : Int => foobd(d,BigDecimal(n2))
              case n2 : Double => foobd(d,BigDecimal(n2))
              case n2 : Float => foobd(d,BigDecimal(n2))
              case n2 : BigInt => foobd(d,BigDecimal(n2))
              case n2 : BigDecimal => foobd(d,n2)
              case n2 : String => toNumOption(n2) match {
                case Some(d2) => foobd(d,d2)
                case None => foos(stringFromAny(n1), stringFromAny(y))
              }
              case n2 : Node => numericFromNode(n2) match {
                case Some(d2) => foobd(d,d2)
                case None => foos(stringFromAny(n1), stringFromAny(y))
              }
              case _ => throw new SparqlEvaluationException("Arguments were not orderable")
            }
            case None => foos(stringFromAny(n1), stringFromAny(y))

          }
        }
        case _ => throw new SparqlEvaluationException("Arguments were not numeric")
      
    }
    Expr2LogicalFilter(processExpression(e.getArg1()), processExpression(e.getArg2()), applyFoo)
  }

  private def boolean2(e : ExprFunction2, foo : (Boolean, Boolean) => Boolean) = {
    Expr2LogicalFilter(processExpression(e.getArg1()), processExpression(e.getArg2()), (x,y) => foo(booleanFromAny(x), booleanFromAny(y)))
  }

  private def string2(e : ExprFunction2, foo : (String, String) => String) = {
    def applyFoo(x : Any, y : Any) : Any = x match {
      case n1 : String => y match {
        case n2 : String => foo(n1,n2)
        case n2 : Node => applyFoo(n1, anyFromNode(n2))
        case _ => throw new SparqlEvaluationException("Arguments were not string")
      }
      case n1 : Node => applyFoo(anyFromNode(n1), y)
      case _ => throw new SparqlEvaluationException("Arguments were not string")
    }
    Expr2Filter(processExpression(e.getArg1()), processExpression(e.getArg2()), applyFoo)
  }

  private def string2l(e : ExprFunction2, foo : (String, String) => Boolean) =  {
    def applyFoo(x : Any, y : Any) : Boolean = x match {
      case n1 : String => y match {
        case n2 : String => foo(n1,n2)
        case n2 : Node => applyFoo(n1, anyFromNode(n2))
        case _ => throw new SparqlEvaluationException("Arguments were not string")
      }
      case n1 : Node => applyFoo(anyFromNode(n1), y)
      case _ => throw new SparqlEvaluationException("Arguments were not string")
    }
    Expr2LogicalFilter(processExpression(e.getArg1()), processExpression(e.getArg2()), applyFoo)
  }

  private [functionalsparql] def processE_Add(e :E_Add) = {
    def add[T](x : T, y : T)(implicit n : Numeric[T]) = n.plus(x,y)
    numeric2(e, add, add, add, add, add)
  }
  private [functionalsparql] def processE_BNode(e : E_BNode) = processExpression(e.getArg(1)) // Why is this function n ??
  private [functionalsparql] def processE_Bound(e : E_Bound) = Expr1LogicalFilter(processExpression(e.getArg()), (x : Any) => !x.isInstanceOf[Var])
  private [functionalsparql] def processE_Call(e : E_Call) = throw new UnsupportedOperationException("No custom function calls supported")
  private [functionalsparql] def processE_Cast(e : E_Cast) = throw new UnsupportedOperationException("TODO (SPARQL 1.1 Feature?)")
  private [functionalsparql] def processE_Coalesce(e : E_Coalesce) = throw new UnsupportedOperationException("TODO (SPARQL 1.1. Feature)")
  private [functionalsparql] def processE_Conditional(e : E_Conditional) = Expr3Filter(processExpression(e.getArg1()), processExpression(e.getArg2()), processExpression(e.getArg3), {
    (x:Any,y:Any,z:Any) => x match {
      case b : Boolean => if(b) { y } else { z}
      case _ => throw new SparqlEvaluationException("Arguments were not boolean")
    }
  })
  private [functionalsparql] def processE_Datatype(e : E_Datatype) = Expr1Filter(processExpression(e.getArg()), (x:Any) => x match {
    case x : Node => if(x.isLiteral) {
      x.getLiteralDatatypeURI() match {
        case null => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#string") // TODO: Should we return langString also?
        case dt => NodeFactory.createURI(dt)
      }
    } else if(x.isURI()) {
      NodeFactory.createURI("uri")
    } else if(x.isVariable()) {
      NodeFactory.createURI("unbound")
    } else { 
      NodeFactory.createURI("bnode")
    }
    case i : Int => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#integer")
    case i : Double => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#double")
    case i : Float => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#float")
    case i : BigDecimal => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#decimal")
    case i : BigInt => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#integer")
    case i : String => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#string")
    case i : Boolean => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#boolean")
    case d : java.util.Date => NodeFactory.createURI("http://www.w3.org/2001/XMLSchema#dateTime")
    case bc : BadCast => "badcast"
    case _ => throw new SparqlEvaluationException("Datatype called on non-node")
  })
  private [functionalsparql] def processE_DateTimeDay(e : E_DateTimeDay) = processExpression(e.getArg())
  private [functionalsparql] def processE_DateTimeHours(e : E_DateTimeHours) = processExpression(e.getArg())
  private [functionalsparql] def processE_DateTimeMinutes(e : E_DateTimeMinutes) = processExpression(e.getArg())
  private [functionalsparql] def processE_DateTimeMonth(e : E_DateTimeMonth) = processExpression(e.getArg())
  private [functionalsparql] def processE_DateTimeSeconds(e : E_DateTimeSeconds) = processExpression(e.getArg())
  private [functionalsparql] def processE_DateTimeTimezone(e : E_DateTimeTimezone) = processExpression(e.getArg())
  private [functionalsparql] def processE_DateTimeTZ(e : E_DateTimeTZ) = processExpression(e.getArg())
  private [functionalsparql] def processE_DateTimeYear(e : E_DateTimeYear) = processExpression(e.getArg())
  private [functionalsparql] def processE_Divide(e : E_Divide) = {
    def divideF[T](x : T, y : T)(implicit n : Fractional[T]) = n.div(x,y)
    def divideI[T](x : T, y : T)(implicit n : Integral[T]) = n.quot(x,y)
    numeric2(e, divideI, divideF, divideF, divideI, divideF)
  }
  private [functionalsparql] def processE_Equals(e : E_Equals) = {
    def checkEq(x : Any, y : Any) : Boolean = {
      x match {
        case n1 : Node => 
          if(y.isInstanceOf[BadCast]) {
            false
          } else {
            if(n1.isBlank()) {
              //!y.isInstanceOf[Node] || !y.asInstanceOf[Node].isLiteral()
              y.isInstanceOf[Node] && y.asInstanceOf[Node].isBlank()
            } else {
              checkEq(anyFromNode(n1),y) 
            }
          }
        case bc : BadCast =>
          val n3 = y match {
            case n2 : Node => 
              if(!n2.isBlank()) {
                anyFromNode(n2) 
              } else {
                null
              }
            case _ => y
          }
          n3 match {
            case bc2 : BadCast =>
              bc.original == bc2.original
            case _ =>
              false
          }
        case _ => y match {
          case n2 : Node => 
            if(n2.isBlank()) {
              false
            //  !x.isInstanceOf[Node] || !x.asInstanceOf[Node].isLiteral()
            } else {
              val n3 = anyFromNode(n2)
              !n3.isInstanceOf[BadCast] && x == n3
            }
          case bc : BadCast => 
            false
          case _ => x == y
        }
      }
    }
    Expr2LogicalFilter(processExpression(e.getArg1()), processExpression(e.getArg2()), checkEq)
  }
  private [functionalsparql] def processE_Exists(e : E_Exists) = throw new UnsupportedOperationException("TODO (Sparql 1.1. Feature) Please rewrite without filter!")
  private [functionalsparql] def processE_Function(e : E_Function) = {
    e.getFunctionIRI() match {
      case "http://www.w3.org/2001/XMLSchema#integer" => Expr1Filter(processExpression(e.getArg(1)), (x:Any) => x match {
        case i : Int => i
        case f : Float => f.toInt
        case d : Double => d.toInt
        case bi : BigInt => bi
        case bd : BigDecimal => bd.toInt
        case any => try {
          stringFromAny(any).toInt
        } catch {
          case x : NumberFormatException => BadCast(any, x)
        }
      })
      case "http://www.w3.org/2001/XMLSchema#boolean" => Expr1Filter(processExpression(e.getArg(1)), (x:Any) => booleanFromAny(x))
      case "http://www.w3.org/2001/XMLSchema#float" => Expr1Filter(processExpression(e.getArg(1)), (x:Any) => x match {
        case i : Int => i.toFloat
        case f : Float => f
        case d : Double => d.toFloat
        case bi : BigInt => bi.toFloat
        case bd : BigDecimal => bd.toFloat
        case any => try {
          stringFromAny(any).toFloat
        } catch {
          case x : NumberFormatException => BadCast(any, x)
        }
      })
      case "http://www.w3.org/2001/XMLSchema#double" => Expr1Filter(processExpression(e.getArg(1)), (x:Any) => x match {
        case i : Int => i.toDouble
        case f : Float => f.toDouble
        case d : Double => d
        case bi : BigInt => bi.toDouble
        case bd : BigDecimal => bd.toDouble
        case any => try {
          stringFromAny(any).toDouble
        } catch {
          case x : NumberFormatException => BadCast(any, x)
        }
      })
      case "http://www.w3.org/2001/XMLSchema#decimal" => Expr1Filter(processExpression(e.getArg(1)), (x:Any) => x match {
        case i : Int => BigDecimal(i)
        case f : Float => BigDecimal(f)
        case d : Double => BigDecimal(d)
        case bi : BigInt => BigDecimal(bi)
        case bd : BigDecimal => bd
        case any => try {
          BigDecimal(stringFromAny(any))
        } catch {
          case x : NumberFormatException => BadCast(any, x)
        }
      })
      case "http://www.w3.org/2001/XMLSchema#string" => Expr1Filter(processExpression(e.getArg(1)), stringFromAny)
      case "http://www.w3.org/2001/XMLSchema#dateTime" => Expr1Filter(processExpression(e.getArg(1)), (x:Any) => try {
          parseIso8601Date(stringFromAny(x))
        } catch {
          case cause : Exception => BadCast(x, cause)
      })
      case _ => 
        throw new UnsupportedOperationException("TODO")
    }
  }
  private [functionalsparql] def processE_FunctionDynamic(e : E_FunctionDynamic) = throw new UnsupportedOperationException("TODO")
  private [functionalsparql] def processE_GreaterThan(e : E_GreaterThan) = {
    def gt[T](x : T, y : T)(implicit o : Ordering[T]) = o.gt(x,y)
    order2(e,gt,gt,gt,gt,gt,gt,gt)
  }
  private [functionalsparql] def processE_GreaterThanOrEqual(e : E_GreaterThanOrEqual) = {
    def gteq[T](x : T, y : T)(implicit o : Ordering[T]) = o.gteq(x,y)
    order2(e,gteq,gteq,gteq,gteq,gteq,gteq,gteq)
  }
  private [functionalsparql] def processE_IRI(e : E_IRI) = processExpression(e.getArg())
  private [functionalsparql] def processE_IsBlank(e : E_IsBlank) = Expr1LogicalFilter(processExpression(e.getArg()), (x:Any) => x.isInstanceOf[Node] && x.asInstanceOf[Node].isBlank())
  private [functionalsparql] def processE_IsIRI(e : E_IsIRI) = Expr1LogicalFilter(processExpression(e.getArg()), (x:Any) => x.isInstanceOf[Node] && x.asInstanceOf[Node].isURI())
  private [functionalsparql] def processE_IsLiteral(e : E_IsLiteral) = Expr1LogicalFilter(processExpression(e.getArg()), (x:Any) => x.isInstanceOf[Node] && x.asInstanceOf[Node].isLiteral())
  private [functionalsparql] def processE_IsNumeric(e : E_IsNumeric) = Expr1LogicalFilter(processExpression(e.getArg()), (x:Any) => x.isInstanceOf[Int] || x.isInstanceOf[Float] || x.isInstanceOf[Double])
  private [functionalsparql] def processE_IsURI(e : E_IsURI) = Expr1LogicalFilter(processExpression(e.getArg()), (x:Any) => x.isInstanceOf[Node] && x.asInstanceOf[Node].isLiteral())
  private [functionalsparql] def processE_Lang(e : E_Lang) = Expr1Filter(processExpression(e.getArg()), 
    (x:Any) => x match {
      case n : Node => if(n.isLiteral()) {
        n.getLiteralLanguage()
      } else {
        BadCast("not a literal",null)
      }
      case _ => null
    })
  private [functionalsparql] def processE_LangMatches(e : E_LangMatches) = Expr2LogicalFilter(processExpression(e.getArg1()), processExpression(e.getArg2()), (x:Any,y:Any) => x == y)
  private [functionalsparql] def processE_LessThan(e : E_LessThan) = {
    def lt[T](x : T, y : T)(implicit o : Ordering[T]) = o.lt(x,y)
    order2(e,lt,lt,lt,lt,lt,lt,lt)
  }
  private [functionalsparql] def processE_LessThanOrEqual(e : E_LessThanOrEqual) = {
    def lteq[T](x : T, y : T)(implicit o : Ordering[T]) = o.lteq(x,y)
    order2(e,lteq,lteq,lteq,lteq,lteq,lteq,lteq)
  }
  private [functionalsparql] def processE_LogicalAnd(e : E_LogicalAnd) = boolean2(e, (x,y) => x && y)
  private [functionalsparql] def processE_LogicalNot(e : E_LogicalNot) = boolean1(e, x => !x)
  private [functionalsparql] def processE_LogicalOr(e : E_LogicalOr) = boolean2(e, (x,y) => x || y)
  private [functionalsparql] def processE_MD5(e : E_MD5) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_Multiply(e : E_Multiply) = {
    def times[T](x : T, y : T)(implicit n : Numeric[T]) = n.times(x,y)
    numeric2(e, times, times, times, times, times)
  }
  private [functionalsparql] def processE_NotEquals(e : E_NotEquals) = {
    def checkNeq(x : Any, y : Any) : Boolean = {
      println(x + " == " + y)
      x match {
        case n1 : Node => 
          if(y.isInstanceOf[BadCast]) {
            true
          } else {
            if(n1.isBlank()) {
              //!y.isInstanceOf[Node] || !y.asInstanceOf[Node].isLiteral()
              !(y.isInstanceOf[Node] && y.asInstanceOf[Node].isBlank())
            } else {
              checkNeq(anyFromNode(n1),y) 
            }
          }
        case bc : BadCast =>
          val n3 = y match {
            case n2 : Node => 
              if(!n2.isBlank()) {
                anyFromNode(n2) 
              } else {
                null
              }
            case _ => y
          }
          n3 match {
            case bc2 : BadCast =>
              bc.original != bc2.original
            case _ =>
              true
          }
        case _ => y match {
          case n2 : Node => 
            if(n2.isBlank()) {
              true
            } else {
              val n3 = anyFromNode(n2)
              !n3.isInstanceOf[BadCast] && x != n3
            }
          case bc : BadCast => 
            false
          case _ => x != y
        }
      }
    }
    Expr2LogicalFilter(processExpression(e.getArg1()), processExpression(e.getArg2()), checkNeq)
  }
  private [functionalsparql] def processE_NotExists(e : E_NotExists) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature) Please rewrite without filter!")
  private [functionalsparql] def processE_NotOneOf(e : E_NotOneOf) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_Now(e : E_Now) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_NumAbs(e : E_NumAbs) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_NumCeiling(e : E_NumCeiling) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_NumFloor(e : E_NumFloor) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_NumRound(e : E_NumRound) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_OneOf(e : E_OneOf) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_OneOfBase(e : E_OneOfBase) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_Random(e : E_Random) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
 
  case class RegexExpressionFilter(e1 : ExpressionFilter, e2 : ExpressionFilter, e3 : ExpressionFilter) extends Expr3LogicalFilter(e1,e2,e3, (x:Any,y:Any,z:Any) => {
    z.toString match {
      case "" => 
        stringFromAny(x).matches(stringFromAny(y))
      case "i" => stringFromAny(x).toLowerCase.matches(stringFromAny(y))
      case _ => throw new UnsupportedOperationException("TODO")
    }
  })
  private [functionalsparql] def processE_Regex(e : E_Regex) = new RegexExpressionFilter(processExpression(e.getArg(1)), processExpression(e.getArg(2)), processExpression(e.getArg(3))) 
  private [functionalsparql] def processE_SameTerm(e : E_SameTerm) = {
    // TODO (not sure why this is different from equals)
    Expr2LogicalFilter(processExpression(e.getArg1()), processExpression(e.getArg2()), (x:Any,y:Any) => x == y)
  }
  private [functionalsparql] def processE_SHA1(e : E_SHA1) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_SHA224(e : E_SHA224) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_SHA256(e : E_SHA256) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_SHA384(e : E_SHA384) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_SHA512(e : E_SHA512) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_Str(e : E_Str) = Expr1Filter(processExpression(e.getArg()), (x:Any) => x.toString)
  private [functionalsparql] def processE_StrAfter(e : E_StrAfter) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrBefore(e : E_StrBefore) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrConcat(e : E_StrConcat) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrContains(e : E_StrContains) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrDatatype(e : E_StrDatatype) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrEncodeForURI(e : E_StrEncodeForURI) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrEndsWith(e : E_StrEndsWith) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrLang(e : E_StrLang) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)") 
  private [functionalsparql] def processE_StrLength(e : E_StrLength) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrLowerCase(e : E_StrLowerCase) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrReplace(e : E_StrReplace) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrStartsWith(e : E_StrStartsWith) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrSubstring(e : E_StrSubstring) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrUpperCase(e : E_StrUpperCase) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrUUID(e : E_StrUUID) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_Subtract(e : E_Subtract) = {
    def minus[T](x : T, y : T)(implicit n : Numeric[T]) = n.minus(x,y)
    numeric2(e, minus, minus, minus, minus, minus)
  }
  private [functionalsparql] def processE_UnaryMinus(e : E_UnaryMinus) = {
    def negate[T](t : T)(implicit n : Numeric[T]) = n.negate(t)
    numeric1(e, negate, negate, negate, negate, negate)
  }
  private [functionalsparql] def processE_UnaryPlus(e : E_UnaryPlus) = processExpression(e.getArg())
  private [functionalsparql] def processE_URI(e : E_URI) = processExpression(e.getArg())
  private [functionalsparql] def processE_UUID(e : E_UUID) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_Version(e : E_Version) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processExprAggregator(e : ExprAggregator) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processExprDigest(e : ExprDigest) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processExprNode(e : ExprNode) = processExpression(e.getExpr())
  private [functionalsparql] def processExprSystem(e : ExprSystem) = throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processExprVar(e : ExprVar) = VariableExpressionFilter(e.asVar())
  private [functionalsparql] def processNodeValueBoolean(e : NodeValueBoolean) = ValueFilter(e.getBoolean())
  private [functionalsparql] def processNodeValueDecimal(e : NodeValueDecimal) = ValueFilter(BigDecimal(e.getDecimal()))
  private [functionalsparql] def processNodeValueDouble(e : NodeValueDouble) = ValueFilter(e.getDouble())
  private [functionalsparql] def processNodeValueDT(e : NodeValueDT) = ValueFilter(e.getDateTime())
  private [functionalsparql] def processNodeValueDuration(e : NodeValueDuration) = ValueFilter(e.getDuration())
  private [functionalsparql] def processNodeValueFloat(e : NodeValueFloat) = ValueFilter(e.getFloat())
  private [functionalsparql] def processNodeValueInteger(e : NodeValueInteger) = ValueFilter(BigInt(e.getInteger()))
  private [functionalsparql] def processNodeValueNode(e : NodeValueNode) = ValueFilter(e.asNode())
  private [functionalsparql] def processNodeValueString(e : NodeValueString) = ValueFilter(e.getString())
}

object FunctionalSparqlUtils {
   def stringFromAny(a : Any) = a match {
    case s : String => s
    case n : Node => if(n.isLiteral()) {
      n.getLiteralValue().toString
      } else if(n.isURI()) {
        n.getURI()
      } else {
        n.toString
      }
    case _ => a.toString
  }
  def booleanFromAny(x : Any) : Boolean= x match {
    case i : Int => i != 0
    case f : Float => f != 0.0
    case d : Double => d != 0.0
    case bi : BigInt => bi != BigInt(0)
    case bd : BigDecimal => bd != BigDecimal(0)
    case n : Node => booleanFromAny(anyFromNode(n))
    case any => stringFromAny(any) match {
      case "true" => true
      case "false" => false
      case "" => false
      case _ => true
    }
  }
  def anyFromNode(n : Node) : Any = if(n.isLiteral()) {
      try {
        n.getLiteralValue() match {
          case i : java.lang.Integer => i.intValue()
          case d : java.lang.Double => d.doubleValue()
          case f : java.lang.Float => f.floatValue()
          case b : java.lang.Boolean => b.booleanValue()
          case s : String => 
            if(n.getLiteralLanguage() != null) {
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
          BadCast(n, x)
      }
    } else if(n.isURI()) {
      URI.create(n.getURI())
    } else if(n.isVariable()) {
      UnboundVariable
    } else {
      throw new SparqlEvaluationException("Cannot convert node to value " + n)
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
}

sealed trait Plan[A] {
  def execute(rdd : DistCollection[Quad]) : A
  def vars : Seq[String]
}

case class SelectPlan(_vars : Seq[String], body : Filter, distinct : Boolean) extends Plan[DistCollection[Match]] {
  def execute(rdd : DistCollection[Quad]) = {
    val matches = body.applyTo(rdd).unique
    val allMatches = matches.map {
      case Match(triples, binding) => 
        Match(Set(), binding.filterKeys(vars.contains(_)))
    }
    if(distinct) {
      allMatches.unique
    } else {
      allMatches
    }
  }
  def vars = _vars
}

case class AskPlan(body : Filter) extends Plan[Boolean] {
  def execute(rdd : DistCollection[Quad]) = {
    val matches = body.applyTo(rdd)
    matches.exists {
      case Match(triples, binding) => true
    }
  }
  def vars = Nil
}

case class DescribePlan(_vars : Seq[Node], body : Filter) extends Plan[DistCollection[Quad]] {
  def execute(rdd : DistCollection[Quad]) = throw new UnsupportedOperationException("TODO")
  def vars = throw new RuntimeException("TODO")
}

case class ConstructPlan(template : BasicPattern, body : Filter) extends Plan[DistCollection[Seq[Quad]]] {
  private def ground(m : Match, r : Node, b : Map[String, Node]) = if(r.isVariable()) {
    b.get(r.asInstanceOf[Var].getVarName()) match {
      case Some(r2) => r2
      case None => throw new SparqlEvaluationException("Variable %s not bound in construct" format (r.asInstanceOf[Var].getVarName()))
    }
  } else {
    r
  }
  def execute(rdd : DistCollection[Quad]) = body.applyTo(rdd).flatMap { 
    case m @ Match(t, b) =>
      Some((template.map { case t => 
        new Quad(
          Quad.defaultGraphIRI,
          ground(m,t.getSubject(),b),
          ground(m,t.getPredicate(),b),
          ground(m,t.getObject(),b)
          )
      }).toSeq)
    case _ => None
  }
  def vars = Nil
}



class StreamRDFCollector extends StreamRDF {
  var triples = collection.mutable.Seq[Quad]()
  override def base(base : String) { }
  override def finish() { }
  override def prefix(prefix : String, iri : String) { }
  override def quad(q : Quad) {
    triples :+= q
  }
  override def start() { }
  override def triple(t : Triple) {
    triples :+= new Quad(Quad.defaultGraphIRI, t)
  }
}

case class BadCast(original : Any, cause : Throwable)


object UnboundVariable

case class SparqlEvaluationException(msg : String = "", cause : Throwable = null) extends RuntimeException(msg,cause)
