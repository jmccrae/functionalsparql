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
import org.joda.time.{DateTime, LocalDate}
import scala.annotation.tailrec
import scala.collection.JavaConversions._

object functionalsparql {
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
  
  def parseQuery(query : String, baseURI : String) : Query = QueryFactory.create(query, baseURI)

  def processQuery(query : String, baseURI : String) : Plan[_] = processQuery(parseQuery(query, baseURI))

  def processQuery(query : Query) : Plan [_]= {
    val graphs = (Option(query.getDatasetDescription()).toSeq.flatMap { dd =>
      dd.getDefaultGraphURIs()
    } map { uri =>
      NodeFactory.createURI(uri)
    }).toSet
    val namedGraphs = (Option(query.getDatasetDescription()).toSeq.flatMap { dd =>
      dd.getNamedGraphURIs()
    } map { uri =>
      NodeFactory.createURI(uri)
    }).toSet
    query.getQueryType() match {
      case Query.QueryTypeAsk => AskPlan(processElement(query.getQueryPattern()), graphs, namedGraphs)
      case Query.QueryTypeConstruct => ConstructPlan(processTemplate(query.getConstructTemplate()), processElement(query.getQueryPattern()), graphs, namedGraphs)
      case Query.QueryTypeDescribe => DescribePlan(query.getResultURIs(),processElement(query.getQueryPattern()), graphs, namedGraphs)
      case Query.QueryTypeSelect => 
        val orderBys = if(query.getOrderBy() != null) {
          query.getOrderBy().map { sc =>
            (processExpression(sc.getExpression()), sc.getDirection())
          }
        } else {
          Nil
        }
        SelectPlan(query.getResultVars().toList,
          processElement(query.getQueryPattern()), query.isDistinct(), graphs,
          namedGraphs, orderBys, query.getOffset(), query.getLimit())
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
      var fs : Expression = TrueFilter
      var g : Filter = NullFilter
      for(e2 <- e.getElements()) {
        e2 match {
          case e3 : ElementFilter => 
            if(fs == TrueFilter) {
              fs = processExpression(e3.getExpr())
            } else {
              fs = LogicAnd(fs, processExpression(e3.getExpr()))
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

  private [functionalsparql] def processExpression(expr : Expr) : Expression = expr match {
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
    case null => null
    case _ => throw new IllegalArgumentException("Bad expression " + expr)
  }
    
  private def toNumOption(s : String) : Option[BigDecimal]  = try {
    Some(BigDecimal(s))
  } catch {
    case x : NumberFormatException => None
  }
  private [functionalsparql] def processE_Add(e :E_Add) = 
    Add(processExpression(e.getArg1()), processExpression(e.getArg2()))
  private [functionalsparql] def processE_BNode(e : E_BNode) = 
    processExpression(e.getArg(1)) // Why is this function n ??
  private [functionalsparql] def processE_Bound(e : E_Bound) = 
    Bound(processExpression(e.getArg()))
  private [functionalsparql] def processE_Call(e : E_Call) = 
    throw new UnsupportedOperationException("No custom function calls supported")
  private [functionalsparql] def processE_Cast(e : E_Cast) = 
    throw new UnsupportedOperationException("TODO (SPARQL 1.1 Feature?)")
  private [functionalsparql] def processE_Coalesce(e : E_Coalesce) = 
    throw new UnsupportedOperationException("TODO (SPARQL 1.1. Feature)")
  private [functionalsparql] def processE_Conditional(e : E_Conditional) = 
    throw new UnsupportedOperationException("TODO (SPARQL 1.1 Feature)")
  private [functionalsparql] def processE_Datatype(e : E_Datatype) = 
    Datatype(processExpression(e.getArg()))
  private [functionalsparql] def processE_DateTimeDay(e : E_DateTimeDay) = 
    processExpression(e.getArg())
  private [functionalsparql] def processE_DateTimeHours(e : E_DateTimeHours) = 
    processExpression(e.getArg())
  private [functionalsparql] def processE_DateTimeMinutes(e : E_DateTimeMinutes) = 
    processExpression(e.getArg())
  private [functionalsparql] def processE_DateTimeMonth(e : E_DateTimeMonth) = 
    processExpression(e.getArg())
  private [functionalsparql] def processE_DateTimeSeconds(e : E_DateTimeSeconds) = 
    processExpression(e.getArg())
  private [functionalsparql] def processE_DateTimeTimezone(e : E_DateTimeTimezone) = 
    processExpression(e.getArg())
  private [functionalsparql] def processE_DateTimeTZ(e : E_DateTimeTZ) = 
    processExpression(e.getArg())
  private [functionalsparql] def processE_DateTimeYear(e : E_DateTimeYear) = 
    processExpression(e.getArg())
  private [functionalsparql] def processE_Divide(e : E_Divide) = 
    Divide(processExpression(e.getArg1()), processExpression(e.getArg2()))
  private [functionalsparql] def processE_Equals(e : E_Equals) = 
    Equals(processExpression(e.getArg1()), processExpression(e.getArg2()))
  private [functionalsparql] def processE_Exists(e : E_Exists) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1. Feature)")
  private [functionalsparql] def processE_Function(e : E_Function) = {
    e.getFunctionIRI() match {
      case "http://www.w3.org/2001/XMLSchema#integer" => 
        CastInt(processExpression(e.getArg(1)))
      case "http://www.w3.org/2001/XMLSchema#boolean" => 
        CastBoolean(processExpression(e.getArg(1)))
      case "http://www.w3.org/2001/XMLSchema#float" => 
        CastFloat(processExpression(e.getArg(1)))
      case "http://www.w3.org/2001/XMLSchema#double" => 
        CastDouble(processExpression(e.getArg(1)))
      case "http://www.w3.org/2001/XMLSchema#decimal" => 
        CastDecimal(processExpression(e.getArg(1)))
      case "http://www.w3.org/2001/XMLSchema#string" => 
        Str(processExpression(e.getArg(1)))
      case "http://www.w3.org/2001/XMLSchema#dateTime" => 
        CastDateTime(processExpression(e.getArg(1)))
      case _ => 
        throw new UnsupportedOperationException("Unsupported function")
    }
  }
  private [functionalsparql] def processE_FunctionDynamic(e : E_FunctionDynamic) = 
    throw new UnsupportedOperationException("TODO (SPARQL 1.1)")
  private [functionalsparql] def processE_GreaterThan(e : E_GreaterThan) = 
    GreaterThan(processExpression(e.getArg1()), processExpression(e.getArg2()))
  private [functionalsparql] def processE_GreaterThanOrEqual(e : E_GreaterThanOrEqual) = 
    GreaterThanEq(processExpression(e.getArg1()), processExpression(e.getArg2()))
  private [functionalsparql] def processE_IRI(e : E_IRI) = 
    processExpression(e.getArg())
  private [functionalsparql] def processE_IsBlank(e : E_IsBlank) = 
    IsBLANK(processExpression(e.getArg()))
  private [functionalsparql] def processE_IsIRI(e : E_IsIRI) = 
    IsURI(processExpression(e.getArg()))
  private [functionalsparql] def processE_IsLiteral(e : E_IsLiteral) = 
    IsLiteral(processExpression(e.getArg()))
  private [functionalsparql] def processE_IsNumeric(e : E_IsNumeric) = 
    throw new UnsupportedOperationException("TODO (SPARQL 1.1 Feature)")
  private [functionalsparql] def processE_IsURI(e : E_IsURI) = 
    IsURI(processExpression(e.getArg()))
  private [functionalsparql] def processE_Lang(e : E_Lang) = 
    LangExpression(processExpression(e.getArg()))
  private [functionalsparql] def processE_LangMatches(e : E_LangMatches) = 
    LangMatches(processExpression(e.getArg1()), processExpression(e.getArg2()))
  private [functionalsparql] def processE_LessThan(e : E_LessThan) = 
    LessThan(processExpression(e.getArg1()), processExpression(e.getArg2()))
  private [functionalsparql] def processE_LessThanOrEqual(e : E_LessThanOrEqual) = 
    LessThanEq(processExpression(e.getArg1()), processExpression(e.getArg2()))
  private [functionalsparql] def processE_LogicalAnd(e : E_LogicalAnd) = 
    LogicAnd(processExpression(e.getArg1()), processExpression(e.getArg2()))
  private [functionalsparql] def processE_LogicalNot(e : E_LogicalNot) = 
    UnaryNot(processExpression(e.getArg()))
  private [functionalsparql] def processE_LogicalOr(e : E_LogicalOr) = 
    LogicOr(processExpression(e.getArg1()), processExpression(e.getArg2()))
  private [functionalsparql] def processE_MD5(e : E_MD5) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_Multiply(e : E_Multiply) = 
    Multiply(processExpression(e.getArg1()), processExpression(e.getArg2()))
  private [functionalsparql] def processE_NotEquals(e : E_NotEquals) = 
    UnaryNot(Equals(processExpression(e.getArg1()), processExpression(e.getArg2())))
  private [functionalsparql] def processE_NotExists(e : E_NotExists) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_NotOneOf(e : E_NotOneOf) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_Now(e : E_Now) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_NumAbs(e : E_NumAbs) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_NumCeiling(e : E_NumCeiling) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_NumFloor(e : E_NumFloor) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_NumRound(e : E_NumRound) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_OneOf(e : E_OneOf) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_OneOfBase(e : E_OneOfBase) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_Random(e : E_Random) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_Regex(e : E_Regex) = 
    Regex(processExpression(e.getArg(1)), processExpression(e.getArg(2)), Option(processExpression(e.getArg(3)))) 
  private [functionalsparql] def processE_SameTerm(e : E_SameTerm) = {
    SameTerm(processExpression(e.getArg1()), processExpression(e.getArg2()))
  }
  private [functionalsparql] def processE_SHA1(e : E_SHA1) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_SHA224(e : E_SHA224) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_SHA256(e : E_SHA256) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_SHA384(e : E_SHA384) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_SHA512(e : E_SHA512) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_Str(e : E_Str) = 
    Str(processExpression(e.getArg()))
  private [functionalsparql] def processE_StrAfter(e : E_StrAfter) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrBefore(e : E_StrBefore) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrConcat(e : E_StrConcat) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrContains(e : E_StrContains) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrDatatype(e : E_StrDatatype) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrEncodeForURI(e : E_StrEncodeForURI) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrEndsWith(e : E_StrEndsWith) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrLang(e : E_StrLang) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)") 
  private [functionalsparql] def processE_StrLength(e : E_StrLength) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrLowerCase(e : E_StrLowerCase) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrReplace(e : E_StrReplace) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrStartsWith(e : E_StrStartsWith) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrSubstring(e : E_StrSubstring) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrUpperCase(e : E_StrUpperCase) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_StrUUID(e : E_StrUUID) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_Subtract(e : E_Subtract) = 
    Subtract(processExpression(e.getArg1()), processExpression(e.getArg2()))
  private [functionalsparql] def processE_UnaryMinus(e : E_UnaryMinus) = 
    UnaryNeg(processExpression(e.getArg()))
  private [functionalsparql] def processE_UnaryPlus(e : E_UnaryPlus) = 
    UnaryPlus(processExpression(e.getArg()))
  private [functionalsparql] def processE_URI(e : E_URI) = 
    processExpression(e.getArg())
  private [functionalsparql] def processE_UUID(e : E_UUID) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processE_Version(e : E_Version) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processExprAggregator(e : ExprAggregator) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processExprDigest(e : ExprDigest) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processExprNode(e : ExprNode) = 
    processExpression(e.getExpr())
  private [functionalsparql] def processExprSystem(e : ExprSystem) = 
    throw new UnsupportedOperationException("TODO (Sparql 1.1 Feature)")
  private [functionalsparql] def processExprVar(e : ExprVar) = 
    VariableExpression(e.asVar().getVarName())
  private [functionalsparql] def processNodeValueBoolean(e : NodeValueBoolean) = 
    LiteralExpression(Logic(e.getBoolean().booleanValue()))
  private [functionalsparql] def processNodeValueDecimal(e : NodeValueDecimal) = 
    LiteralExpression(BigDecimal(e.getDecimal()))
  private [functionalsparql] def processNodeValueDouble(e : NodeValueDouble) = 
    LiteralExpression(e.getDouble())
  private [functionalsparql] def processNodeValueDT(e : NodeValueDT) = if(e.isDateTime()) {
      LiteralExpression(DateTime.parse(e.getDateTime().toString()))//e.getDateTime().toGregorianCalendar().getTime())
    } else {
      LiteralExpression(LocalDate.parse(e.getDateTime().toString()))//.toGregorianCalendar().getTime()))
    }
  private [functionalsparql] def processNodeValueDuration(e : NodeValueDuration) = 
    LiteralExpression(e.getDuration())
  private [functionalsparql] def processNodeValueFloat(e : NodeValueFloat) = 
    LiteralExpression(e.getFloat())
  private [functionalsparql] def processNodeValueInteger(e : NodeValueInteger) = 
    LiteralExpression(e.getInteger().intValue())
  private [functionalsparql] def processNodeValueNode(e : NodeValueNode) = 
    LiteralExpression(e.asNode())
  private [functionalsparql] def processNodeValueString(e : NodeValueString) = 
    LiteralExpression(e.getString())
}

sealed trait Plan[A] {
  def execute(rdd : DistCollection[Quad]) : A
  def vars : Seq[String]
  def graphs : Set[Node]
  def namedGraphs : Set[Node]
  protected def mapDefaultGraph(rdd : DistCollection[Quad]) = if(graphs.isEmpty) {
    rdd
  } else if(namedGraphs.isEmpty) {
    rdd.map { q =>
      if(graphs.contains(q.getGraph())) {
        new Quad(Quad.defaultGraphIRI, q.getSubject(), q.getPredicate(), q.getObject())
      } else {
        q
      }
    }
  } else {
    rdd.flatMap { q =>
      if(graphs.contains(q.getGraph()) && namedGraphs.contains(q.getGraph())) {
        // A blank node in a named graph is also not the same as that blank node in 
        // the default graph... I doubt a real user would ever understand this difference
        val s = if(q.getSubject().isBlank()) {
          NodeFactory.createAnon()
        } else {
          q.getSubject()
        }
        val o = if(q.getObject().isBlank()) {
          NodeFactory.createAnon()
        } else {
          q.getObject()
        }
        List(q, new Quad(Quad.defaultGraphIRI, s, q.getPredicate(), o))
      } else if(graphs.contains(q.getGraph())) {
        List(new Quad(Quad.defaultGraphIRI, q.getSubject(), q.getPredicate(), q.getObject()))
      } else {
        List(q)
      }
    }
  }
}

case class SelectPlan(_vars : Seq[String], body : Filter, distinct : Boolean, graphs : Set[Node],
  namedGraphs : Set[Node], orderBy : Seq[(Expression, Int)],
  offset : Long = 0, limit : Long = -1) extends Plan[DistCollection[Match]] {
  def execute(rdd : DistCollection[Quad]) = {
    val matches = body.applyTo(mapDefaultGraph(rdd)).unique
    val allMatches = matches.map {
      case Match(triples, binding) => 
        Match(Set(), binding.filterKeys(vars.contains(_)))
    }
    implicit val sparqlOrdering = new SparqlValueOrdering(orderBy.map(_._2 match {
      case -2 => 1 // Not specified
      case 1 => 1
      case -1 => -1
      case x => throw new UnsupportedOperationException("Unexpected ordering value for Jena " + x)
    }))
    val matches2 = if(distinct) {
      allMatches.unique
    } else {
      allMatches
    }
    val matches3 = if(orderBy.isEmpty) {
      matches2
    } else {
      (matches2.key { m =>
        orderBy.map(_._1.yieldValue(m))
      }).sorted
    }
    if(offset > 0 && limit >= 0) {
      matches3.drop(offset).take(limit)
    } else if(offset > 0) {
      matches3.drop(offset)
    } else if(limit >= 0) {
      matches3.take(limit)
    } else {
      matches3
    }
  }
  def vars = _vars
}


case class AskPlan(body : Filter, graphs : Set[Node],
  namedGraphs : Set[Node]) extends Plan[Boolean] {
  def execute(rdd : DistCollection[Quad]) = {
    val matches = body.applyTo(mapDefaultGraph(rdd))
    matches.exists {
      case Match(triples, binding) => true
    }
  }
  def vars = Nil
}

case class DescribePlan(_vars : Seq[Node], body : Filter, graphs : Set[Node],
  namedGraphs : Set[Node]) extends Plan[DistCollection[Quad]] {
  def execute(rdd : DistCollection[Quad]) = throw new UnsupportedOperationException("TODO")
  def vars = throw new RuntimeException("TODO")
}

case class ConstructPlan(template : BasicPattern, body : Filter, graphs : Set[Node],
  namedGraphs : Set[Node]) extends Plan[DistCollection[Seq[Quad]]] {
  private def ground(m : Match, r : Node, b : Map[String, Node]) = if(r.isVariable()) {
    b.get(r.asInstanceOf[Var].getVarName()) match {
      case Some(r2) => r2
      case None => throw new SparqlEvaluationException("Variable %s not bound in construct" format (r.asInstanceOf[Var].getVarName()))
    }
  } else {
    r
  }
  def execute(rdd : DistCollection[Quad]) = body.applyTo(mapDefaultGraph(rdd)).flatMap { 
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

case class SparqlEvaluationException(msg : String = "", cause : Throwable = null) extends RuntimeException(msg,cause)
