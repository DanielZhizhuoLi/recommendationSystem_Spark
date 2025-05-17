import scala.io.Source
import scala.util.Random
import scala.collection.mutable
import java.io.PrintWriter
import java.io.File
import Blackbox._  

object task2 {
  val BIT_ARRAY_SIZE: Int = 69997 
  val NUM_HASH_FUNCTIONS: Int = 10

  //   f(x) = ((a * x + b) % p) % m
  def generateHash(m: Int = BIT_ARRAY_SIZE, numHush: Int = NUM_HASH_FUNCTIONS): List[BigInt => Int] = {


    val primeList = List(1999, 2003, 2011, 2017, 2027, 2029, 2039, 2053, 2063, 2069)
    (1 to numHush).map { _ =>
      val p = primeList(Random.nextInt(primeList.length))
      val a = Random.nextInt(p - 1) + 1 // random integer in [1, p-1]
      val b = Random.nextInt(p - 1) + 1
      (x: BigInt) => (((a * x + b) % p) % m).toInt
    }.toList
  }


  val globalHashFunctions: List[BigInt => Int] = generateHash()


  def myhashs(s: String): Seq[Int] = {
    val inputNumber = BigInt(s.getBytes("UTF-8"))
    globalHashFunctions.map(hf => hf(inputNumber))
  }


  def countTrailingZeros(x: Int): Int = {
    var count = 0
    var y = x
    if (y == 0) return 0 // safeguard; ideally, x should not be 0.
    while (y % 2 == 0) {
      count += 1
      y = y / 2
    }
    count
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Usage: Task1 <input_file> <stream_size> <num_of_asks> <output_file>")
      sys.exit(1)
    }

    val inputFile = args(0)
    val streamSize = args(1).toInt
    val numOfAsks = args(2).toInt
    val outputFile = args(3)

    val bx = Blackbox()
    
    // Open output CSV file.
    val pw = new PrintWriter(new File(outputFile))
    pw.println("Time,Ground Truth,Estimation")
    
    var totalEstimation: Double = 0.0
    var totalGroundTruth: Double = 0.0

    for (i <- 0 until numOfAsks) {

      val stream: Array[String] = bx.ask(inputFile, streamSize)
      val groundTruth = stream.distinct.length
      totalGroundTruth += groundTruth

      val R: Array[Int] = Array.fill(NUM_HASH_FUNCTIONS)(0)
      
  
      for (user <- stream) {
        val hvals: Seq[Int] = myhashs(user)
        hvals.zipWithIndex.foreach { case (h, j) =>
          val tz = countTrailingZeros(h)
          if (tz > R(j)) R(j) = tz
        }
      }
      
      
      val estimates: Seq[Double] = R.map(r => math.pow(2, r))
      


      val groupSize = 10
      val groupAvgs: Seq[Double] = estimates.grouped(groupSize).map { group =>
        group.sum / group.size.toDouble
      }.toSeq.sorted
      

      val n = groupAvgs.length
      val fmEstimate: Double = 
        if (n % 2 == 1) groupAvgs(n / 2)
        else (groupAvgs(n / 2 - 1) + groupAvgs(n / 2)) / 2.0
      
      // Convert fmEstimate to an integer by rounding.
      val fmEstimateInt: Int = math.round(fmEstimate).toInt
      
      totalEstimation += fmEstimateInt
      
      pw.println(s"$i,$groundTruth,$fmEstimateInt")
    }

    pw.close()

    if (totalGroundTruth > 0) {
      println(s"\nOverall ratio (sum of estimations / sum of ground truths): ${totalEstimation / totalGroundTruth}")
    } else {
      println("\nNo ground truth values were accumulated.")
    }
  }
}
