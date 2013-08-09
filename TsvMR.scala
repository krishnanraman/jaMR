import java.io.{File,PrintWriter}
import scala.io.Source
import collection.mutable.ListBuffer

object Tsv {

  // size of 1 batch of data
  val N = 1024

  // ops
  val MAPPER = 1
  val PROJECT = 2
  val DISCARD = 3
  val REDUCER = 4
  val FILTER = 5

  type T = (Seq[Double] => Double)
  type B = (Seq[Double] => Boolean)

  def mk(data:Iterator[Seq[Double]], input:String, output:String) = {
    writePart(data, ".", input)
    Tsv(input,output)
  }

  private def writePart(rows:Iterator[Seq[Double]], parent:String, filename:String) = {
    new File(parent).mkdir
    write(rows, new PrintWriter(new File(parent, filename)))
  }

  private def write(rows:Iterator[Seq[Double]], pw:PrintWriter) = {
    rows.foreach(row => pw.println(row.mkString("\t")))
    pw.flush
    pw.close
  }
}

case class Tsv(input:String, output:String) {

  private type LB = (Int,Option[Tsv.T], Option[Seq[Int]], Option[Tsv.B])
  private val flow: ListBuffer[LB] = new ListBuffer()

  def map(mapper: Tsv.T) = {
    val temp = (Tsv.MAPPER, Some(mapper), None, None)
    flow += temp
    this
  }

  def filter(filter: Tsv.B) = {
    val temp = (Tsv.FILTER, None, None, Some(filter))
    flow += temp
    this
  }

  def reduce(col:Int, reducer: Tsv.T) = {
    val temp = (Tsv.REDUCER, Some(reducer), None, None)
    flow += temp
    this
  }

  def project(cols:Seq[Int]) = {
    val temp = (Tsv.PROJECT, None, Some(cols), None)
    flow += temp
    this
  }

  def discard(cols:Seq[Int]) = {
    val temp = (Tsv.DISCARD, None, Some(cols), None)
    flow += temp
    this
  }

  private def runFlow(rows:Seq[String]) = {
    // run flow in sequence for all rows then write to part file
    rows.map { row =>

      // each row is a bunch of strings - convert to a sequence of doubles
      val init: Option[Seq[Double]] = Some(row.split("\t").map(_.toDouble))

      flow.foldLeft(init) {
        (arrayOpt,mapper) => {
            val (id, mOpt, colsOpt, filterOpt) = mapper
            if(!arrayOpt.isDefined) None
            else {
              val array = arrayOpt.get
              id match {
                case Tsv.PROJECT =>
                  val cols = colsOpt.get
                  Some(cols.map { i=>array(i) } )

                case Tsv.DISCARD =>
                  val cols = colsOpt.get
                  val res = (0 until array.size)
                              .toArray
                              .filterNot { i=>cols.contains(i) }
                              .map { i=>array(i) }
                            Some(res)

                case Tsv.MAPPER =>
                  val m = mOpt.get
                  Some(array ++ Array(m.apply(array)))

                case Tsv.FILTER =>
                  val filter = filterOpt.get
                  if(filter.apply(array)) arrayOpt else None
              }
            } // else
          }
        }.flatten.toSeq
    }.toIterator
  }

  def start: Unit = {
    val init = System.currentTimeMillis
    val iter = Source.fromFile(input).getLines.grouped(Tsv.N)
    var part = 0
    while(iter.hasNext) {
      val rows = iter.next.toSeq
      part += 1
      Tsv.writePart(runFlow(rows), output, part.toString)
    }
    println("Job completed in " + (System.currentTimeMillis - init) + " ms.")
  }
}

object TsvMR extends App {

  // some mappers - notice the variety of styles you could use here
  def sum: Tsv.T = _.sum // add the first three args
  def prod_minus_sum(a:Seq[Double]) =  a.init.product - a.last // multiply the first three args and subtract the fourth
  def hash: Tsv.T = _.head % 10
  def nonzeros: Tsv.B = _.last != 0.0d
  def times5: Tsv.T = _.last * 5.0

  // lets make some row & columns
  // each row looks like (a,a+1,a+2)
  // ie. each row has exactly 3 columns
  // lets make n such rows, n is cmd line arg
  val n = args(0).toInt
  val data = (1 to n).toIterator.map(row => Seq(row+0.0d, row+1.0d, row+2.0d))

  // make a Tsv file "input" that has the above data
  // write the results of the job to "result"
  Tsv.mk(data, "input", "result")

  // specify mappers
  .map( sum )
  .map( prod_minus_sum _ )
  .project(Seq(4))  //alternately, .discard(Seq(0,1,2,3))
  .map( hash )
  .filter( nonzeros )
  .map( times5 )
  .start
}
