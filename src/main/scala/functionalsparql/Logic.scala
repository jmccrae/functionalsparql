package eu.liderproject.functionalsparql

sealed trait Logic {
  def unary_! : Logic
  def &&(l : Logic) : Logic
  def ||(l : Logic) : Logic
  // The 'monad' operator, x >= y = x if x = Error, x >= y = y otherwise
  def >=(l : Logic) : Logic
  def compareTo(l : Logic) : Int
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
  def >=(l : Logic) = l 
  def compareTo(l  : Logic) = l match {
    case True => 0
    case False => +1
    case Error => -1
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
  def >=(l : Logic) = l
  def compareTo(l  : Logic) = l match {
    case True => -1
    case False => 0
    case Error => -1
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
  def >=(l : Logic) = Error
  def compareTo(l  : Logic) = l match {
    case True => +1
    case False => +1
    case Error => 0
  }
  override def toString = "error"
}


