import org.apache.spark.sql.SparkSession
import scala.math._

object task2_1 {

  def computePearson(ratingsB: Map[String, Double], ratingsI: Map[String, Double], minCommon: Int = 1): Double = {
    val commonUsers = ratingsB.keySet.intersect(ratingsI.keySet)
    if (commonUsers.size < minCommon) return 0.0

    val avgB = commonUsers.map(ratingsB).sum / commonUsers.size
    val avgI = commonUsers.map(ratingsI).sum / commonUsers.size

    val numerator = commonUsers.map(u => (ratingsB(u) - avgB) * (ratingsI(u) - avgI)).sum
    val denominatorB = sqrt(commonUsers.map(u => pow(ratingsB(u) - avgB, 2)).sum)
    val denominatorI = sqrt(commonUsers.map(u => pow(ratingsI(u) - avgI, 2)).sum)
    val denominator = denominatorB * denominatorI

    if (denominator == 0.0) {
      if (numerator == 0.0) 1.0 else -1.0
    } else {
      math.min(1.0, numerator / denominator)
    }
  }

  def predictRating(user: String, business: String,
                    businessRatings: Map[String, Map[String, Double]],
                    userRatings: Map[String, Map[String, Double]],
                    businessMeans: Map[String, Double],
                    userMeans: Map[String, Double],
                    overallAvg: Double,
                    neighborLimit: Int = 50): Double = {

    if (!businessRatings.contains(business)) {
      return userRatings.get(user).map(_ => userMeans.getOrElse(user, overallAvg)).getOrElse(overallAvg)
    }
    if (!userRatings.contains(user)) {
      return businessMeans.getOrElse(business, overallAvg)
    }

    val neighborList = userRatings(user).toList.flatMap { case (b_i, rating) =>
      if (b_i == business || !businessRatings.contains(b_i)) None else {
        val sim = computePearson(businessRatings(business), businessRatings(b_i)) * (if (computePearson(businessRatings(business), businessRatings(b_i)) < 0) 0.8 else 1.2)
        val normalizedRating = rating - (businessMeans.getOrElse(b_i, overallAvg) + userMeans.getOrElse(user, overallAvg) - overallAvg)
        Some((sim, normalizedRating))
      }
    }.sortBy(-_._1).take(neighborLimit)

    val numerator = neighborList.map { case (sim, rating) => sim * rating }.sum
    val denominator = neighborList.map { case (sim, _) => abs(sim) }.sum

    val baseline = overallAvg +
      (businessMeans.getOrElse(business, overallAvg) - overallAvg) +
      (userMeans.getOrElse(user, overallAvg) - overallAvg)

    val prediction = if (denominator != 0) baseline + numerator / denominator else baseline
    math.max(1.0, math.min(5.0, prediction))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("task2_1").getOrCreate()
    val sc = spark.sparkContext

    val trainFile = args(0)
    val testFile = args(1)
    val outputFile = args(2)

    val rdd = sc.textFile(trainFile)
    val header = rdd.first()

    val train = rdd.filter(_ != header).map(_.split(",")).cache()

    val businessRatings = train.map(x => (x(1), (x(0), x(2).toDouble)))
      .groupByKey()
      .mapValues(_.toMap)

    val businessMeans = businessRatings.mapValues(v => v.values.sum / v.size).collect().toMap

    val userRatings = train.map(x => (x(0), (x(1), x(2).toDouble)))
      .groupByKey()
      .mapValues(_.toMap)

    val userMeans = userRatings.mapValues(v => v.values.sum / v.size).collect().toMap

    val overallAvg = train.map(_(2).toDouble).mean()

    // 使用广播变量
    val broadcastBusinessRatings = sc.broadcast(businessRatings.collect().toMap)
    val broadcastUserRatings = sc.broadcast(userRatings.collect().toMap)
    val broadcastBusinessMeans = sc.broadcast(businessMeans)
    val broadcastUserMeans = sc.broadcast(userMeans)
    val broadcastOverallAvg = sc.broadcast(overallAvg)

    val testData = sc.textFile(testFile)
      .filter(_ != header)
      .map(_.split(",")).map(x => (x(0), x(1)))

    val output = testData.map { case (user, business) =>
      val rating = predictRating(user, business, broadcastBusinessRatings.value, broadcastUserRatings.value, broadcastBusinessMeans.value, broadcastUserMeans.value, broadcastOverallAvg.value)
      (user, business, rating)
    }.collect()

    val writer = new java.io.PrintWriter(outputFile)
    writer.println("user_id,business_id,prediction")
    output.foreach { case (u, b, p) =>
      writer.println(s"$u,$b,$p")
    }
    writer.close()

    train.unpersist() // 释放缓存资源
    spark.stop()
  }
}