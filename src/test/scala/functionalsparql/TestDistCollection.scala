package eu.liderproject.functionalsparql

import org.scalatest._

class TestDistCollection extends FlatSpec with Matchers {
	"cogrouping" should "work" in {
		val c1 = SimpleKeyDistCollection(Seq(
			(1,2),(3,4),(2,5),(1,7),(60,1)
			))
		val c2 = SimpleKeyDistCollection(Seq(
			(3,4),(3,4),(2,2),(-1,1)
			)) 
		val SimpleDistCollection(seq) = c1.cogroup(c2).filter{
			case (x,y) => !x.isEmpty && !y.isEmpty
		}
		val iter = seq.iterator
		
		{
			val (_i1, _i2) = iter.next
			val i1 = _i1.iterator
			val i2 = _i2.iterator
			i1.next should be (5)
			i1.hasNext should be (false)
			i2.next should be (2)
			i2.hasNext should be (false)
		}
		{
			val (_i1, _i2) = iter.next
			val i1 = _i1.iterator
			val i2 = _i2.iterator
			i1.next should be (4)
			i1.hasNext should be (false)
			i2.next should be (4)
			i2.next should be (4)
			i2.hasNext should be (false)
		}
		iter.hasNext should be (false)

	}
}