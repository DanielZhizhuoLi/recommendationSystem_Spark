import scala.io.Source
import scala.util.Random
import scala.collection.mutable
import java.io.PrintWriter
import java.io.File

import Blackbox._  

object task1 {


  val BIT_ARRAY_SIZE: Int = 69997         
  val NUM_HASH_FUNCTIONS: Int = 10      


  val bitArray: Array[Int] = Array.fill(BIT_ARRAY_SIZE)(0)

  val trueUsers: mutable.Set[String] = mutable.Set.empty[String]


  def generateHash(m: Int = BIT_ARRAY_SIZE, numHush: Int = NUM_HASH_FUNCTIONS): List[BigInt => Int] = {
    val primeList = List(50021, 50023, 50033, 50047, 50051, 50069, 50077, 50087, 50093, 50101)
    val hashFunctions = (1 to numHush).map { _ =>
      val p = primeList(Random.nextInt(primeList.length))

      val a = Random.nextInt(p - 1) + 1
      val b = Random.nextInt(p - 1) + 1

      (x: BigInt) => (((a * x + b) % p) % m).toInt
    }.toList
    hashFunctions
  }

  // Global hash functions generated once
  val globalHashFunctions: List[BigInt => Int] = generateHash()


  def myhashs(s: String): List[Int] = {
    val inputNumber = BigInt(s.getBytes("UTF-8"))
    globalHashFunctions.map(hf => hf(inputNumber))
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      sys.exit(1)
    }

    val inputFile = args(0)
    val streamSize = args(1).toInt
    val numOfAsks = args(2).toInt
    val outputFile = args(3)


    val bx = Blackbox()
    
    val pw = new PrintWriter(new File(outputFile))
    pw.println("Time,FPR")

    // Process each round of the data stream
    for (i <- 0 until numOfAsks) {
     
      val userStream: Array[String] = bx.ask(inputFile, streamSize)
      var falsePositive: Double = 0.0  
      var totalNegative: Double = 0.0 

      for (userId <- userStream) {
        // Use the global hash functions to compute hash values for the user
        val hashes: List[Int] = myhashs(userId)
        // Check if all positions in the bit array corresponding to these hash values are set to 1
        val seen: Boolean = hashes.forall(h => bitArray(h) == 1)
        if (seen && !trueUsers.contains(userId)) {
          falsePositive += 1.0
        }

        // If the user has not been seen before, update the counter, trueUsers set, and the bit array.
        if (!trueUsers.contains(userId)) {
          totalNegative += 1.0
          trueUsers.add(userId)
          hashes.foreach { h =>
            bitArray(h) = 1
          }
        }
      }

      val fpr: Double = if (totalNegative > 0) falsePositive / totalNegative else 0.0
      pw.println(s"$i,$fpr")
    }

    pw.close()
  }
}
