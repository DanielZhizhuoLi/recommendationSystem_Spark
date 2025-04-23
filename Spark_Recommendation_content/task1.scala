import org.apache.spark.sql.SparkSession
import scala.util.Random
import scala.math.sqrt
import java.io._


object Task1 {

  def isPrime(n: Int): Boolean = {
    if (n <= 1) false
    else (2 to math.sqrt(n).toInt).forall(n % _ != 0)
  }

  def nextPrime(n: Int): Int = {
    var candidate = n
    while (!isPrime(candidate)) {
      candidate += 1
    }
    candidate
  }

  def computeSignature(userSet: Set[String],
                       userIndex: Map[String, Long],
                       hashFuncs: Seq[Long => Long]): Array[Long] = {

    val sig = Array.fill[Long](hashFuncs.length)(Long.MaxValue)
    for (user <- userSet) {
      userIndex.get(user).foreach { idx =>
        for ((f, i) <- hashFuncs.zipWithIndex) {
          val hVal = f(idx)
          if (hVal < sig(i)) sig(i) = hVal
        }
      }
    }
    sig
  }

  def splitIntoBands(businessId: String, signature: Array[Long], b: Int, r: Int): Seq[((Int, Int), String)] = {
    (0 until b).map { i =>
      val band = signature.slice(i * r, (i + 1) * r).toSeq

      val bandKey = (i, band.hashCode())
      (bandKey, businessId)
    }
  }

  def generateCandidatePairs(businessList: Seq[String]): Seq[(String, String)] = {
    val sortedList = businessList.sorted
    for {
      i <- sortedList.indices
      j <- (i + 1) until sortedList.size
    } yield (sortedList(i), sortedList(j))
  }

  def jaccardSimilarity(users1: Set[String], users2: Set[String]): Double = {
    val intersectionSize = (users1 intersect users2).size
    val unionSize = (users1 union users2).size
    if (unionSize == 0) 0.0 else intersectionSize.toDouble / unionSize
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: Task1 <input_file> <output_file>")
      sys.exit(1)
    }
    val inputFile = args(0)
    val outputFile = args(1)

    val spark = SparkSession.builder().appName("Task1").getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.textFile(inputFile)
    val header = rdd.first()
    
    val dataRdd = rdd.filter(line => line != header)

    val kvRdd = dataRdd.map(line => {
      val fields = line.split(",")
      (fields(1), fields(0))
    })

    val businessSets = kvRdd.mapValues(user => Set(user)).reduceByKey(_ union _)
    val allUsers = kvRdd.map(_._2).distinct()
    val userPermutation = allUsers.zipWithIndex().collectAsMap()


    val numUsers = userPermutation.size
    val m = nextPrime(numUsers)
    val numHashFuncs = 100
    val rand = new Random()
    //  f(x) = (a*x + b) % m 
    val hashFuncs: Seq[Long => Long] = (0 until numHashFuncs).map { _ =>
      val a = rand.nextInt(m - 1) + 1
      val b = rand.nextInt(m)
      (x: Long) => (a * x + b) % m
    }

    // (business_id, signature: Array[Long])
    val businessSignatures = businessSets.mapValues(users => computeSignature(users, userPermutation.toMap, hashFuncs))

    //
    val bNum = 20
    val rNum = 5
    val bandsRdd = businessSignatures.flatMap { case (businessId, signature) =>
      splitIntoBands(businessId, signature, bNum, rNum)
    }
    
    val groupedBands = bandsRdd.groupByKey().mapValues(_.toSeq)
    // 生成候选对：对每个 band 的业务列表，两两组合
    val candidatePairs = groupedBands.flatMap { case (_, businessList) =>
      generateCandidatePairs(businessList)
    }.distinct()

    val businessUsersDict = sc.broadcast(businessSets.collectAsMap())


    val candidateSimilarities = candidatePairs.map { case (b1, b2) =>
      val users1 = businessUsersDict.value(b1)
      val users2 = businessUsersDict.value(b2)
      val sim = jaccardSimilarity(users1, users2)
      (b1, b2, sim)
    }

    val similarPairsSorted = candidateSimilarities.filter(_._3 >= 0.5)
      .sortBy { case (b1, b2, sim) => (b1, b2, sim) }

    val sortedList = similarPairsSorted.collect().toList

  
  
    val writer = new PrintWriter(new File(outputFile))
    writer.println("business_id_1,business_id_2,similarity")
    sortedList.foreach { case (b1, b2, sim) =>
      writer.println(s"$b1,$b2,$sim")
    }
    writer.close()

    spark.stop()
  }
}