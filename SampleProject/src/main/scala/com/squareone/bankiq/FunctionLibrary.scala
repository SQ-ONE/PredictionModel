package com.squareone.bankiq

import java.util.Date

object FunctionLibrary {
  def returnDate(date: String, pattern: String) = {
    val format = new java.text.SimpleDateFormat(pattern)
    format.parse(date)
  }
  def format1(date: String) = {
    returnDate(date,"ddMMMyy")
  }
  def format2(date: String) = {
    returnDate(date,"dd-MMM-yy")
  }
 /* implicit class Parser(s: String) {

    case class ParseOp[T](op: String => T)

    implicit val popDouble = ParseOp[Double](_.toDouble)
    implicit val popInt = ParseOp[Int](_.toInt)
    implicit val popDate = ParseOp[Date](FunctionLibrary.format1(_))

    def parse[T: ParseOp] = try {
      Some(implicitly[ParseOp[T]].op(s))
    }
    catch {
      case _ => None
    }
  }*/
}
