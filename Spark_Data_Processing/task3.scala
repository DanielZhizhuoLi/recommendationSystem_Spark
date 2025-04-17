import org.apache.spark.sql.SparkSession
import scala.util.parsing.json.JSON
import java.io.PrintWriter
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import scala.collection.immutable.ListMap
import org.apache.spark.HashPartitioner
import java.io.File




object task3 {
    def main(args: Array[String]): Unit = {
        
        val spark = SparkSession.builder.appName("task3").getOrCreate()
        val sc = spark.sparkContext

        val m2Start = System.nanoTime
       
        val dfReview = spark.read.json(args(0)).select("business_id", "stars")
        val dfBusiness = spark.read.json(args(1)).select("business_id", "city")


        val outputPathA = args(2)
        val outputPathB = args(3)

        val reviewKV = dfReview.rdd.map(row => (row.getString(0), (row.getAs[Number]("stars").doubleValue(), 1)))
        val businessKV = dfBusiness.rdd.map(row => (row.getString(0), row.getString(1)))

 
        val joinRDD = reviewKV.leftOuterJoin(businessKV)

        val city_star_sum = joinRDD.map { case (businessId, ((stars, count), cityOpt)) => 
            (cityOpt.getOrElse(""), (stars, count))
        }.reduceByKey {

            case ((star1, count1), (star2, count2)) => (star1 + star2, count1 + count2)
        }

        val avg_city_star = city_star_sum.mapValues{
            case (sumStar, sumCount) =>
                sumStar/sumCount.toDouble    

        }.sortBy{
            case (city, avg_star) =>

            (-avg_star, city)


        }.collect()


        avg_city_star.take(10).foreach(println)




        val outputFile = new File(outputPathA)
        val writerA = new PrintWriter(outputFile)

        try {
            writerA.write("city,stars\n")  // Header
            avg_city_star.foreach { case (city, stars) =>
                writerA.write(s"$city,$stars\n")
            }
        } finally {
            writerA.close()  // Close 
        }


        val m2Time = System.nanoTime - m2Start



// Question B
        val m1Start = System.nanoTime

        val dfReview1 = spark.read.json(args(0)).select("business_id", "stars")
        val dfBusiness1 = spark.read.json(args(1)).select("business_id", "city")


        val reviewKV1 = dfReview1.rdd.map(row => (row.getString(0), (row.getAs[Number]("stars").doubleValue(), 1)))
        val businessKV1 = dfBusiness1.rdd.map(row => (row.getString(0), row.getString(1)))
 
        val joinRDD1 = reviewKV1.leftOuterJoin(businessKV1)

        val city_star_sum1 = joinRDD1.map { case (businessId, ((stars, count), cityOpt)) => 
            (cityOpt.getOrElse(""), (stars, count))
        }.reduceByKey {

            case ((star1, count1), (star2, count2)) => (star1 + star2, count1 + count2)
        }

        val avg_city_star1 = city_star_sum1.mapValues{
            case (sumStar, sumCount) =>
                sumStar/sumCount.toDouble    

        }.collect()

        val sorted_avg_city_star = avg_city_star1.sorted(
            Ordering.by[(String, Double), (Double, String)](x => (-x._2, x._1))
        )


        sorted_avg_city_star.take(10).foreach(println)

        val m1Time = System.nanoTime - m1Start


        implicit val formats: Formats = Serialization.formats(NoTypeHints)

        val result: ListMap[String, Any] = ListMap(
            "m1" -> m1Time/ 1e9,
            "m2" -> m2Time/ 1e9,
            "reason" -> "After running on Local, I found using Scala’s sorted function is faster than Spark due to the partition of Spark. When sort the whole data it needs to copy through different partition, this kind of shuffling slows the runtime. "

 
        )
        
        val jsonResult = write(result)


        val writerB = new PrintWriter(outputPathB)
        writerB.write(jsonResult)
        writerB.close()




    
        spark.stop()
    

    }

}