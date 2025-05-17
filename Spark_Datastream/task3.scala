import scala.io.Source
import scala.util.Random
import java.io.PrintWriter
import java.io.File

import Blackbox._  

object task3 {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      Console.err.println("Usage: Task3 <input_file> <stream_size> <num_of_asks> <output_file>")
      sys.exit(1)
    }

    val inputFile   = args(0)
    val streamSize  = args(1).toInt   
    val numOfAsks   = args(2).toInt   
    val outputFile  = args(3)

    Random.setSeed(553)

    val box = Blackbox()

    val reservoirSize = 100
    val reservoir     = new Array[String](reservoirSize)
    var globalCount   = 0


    val pw = new PrintWriter(new File(outputFile))
    pw.println("seqnum,0_id,20_id,40_id,60_id,80_id")

    // Process each
    for (_ <- 0 until numOfAsks) {
      val stream = box.ask(inputFile, streamSize)
      for (user <- stream) {
        globalCount += 1
        if (globalCount <= reservoirSize) {
          
          reservoir(globalCount - 1) = user
        } else {
          
          val p = reservoirSize.toFloat / globalCount.toFloat
          if (Random.nextFloat() < p) {
            // replace a uniform random slot [0..99]
            val idx = Random.nextInt(reservoirSize)
            reservoir(idx) = user
          }
        }
      }

      // After each batch, output seqnum + slots 0,20,40,60,80
      val snapshot = Seq(0,20,40,60,80).map(i => reservoir(i))
      pw.println((globalCount.toString +: snapshot).mkString(","))
    }

    pw.close()
  }
}
