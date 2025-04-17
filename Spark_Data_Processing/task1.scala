import org.apache.spark.sql.SparkSession
import scala.util.parsing.json.JSON
import java.io.PrintWriter
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import scala.collection.immutable.ListMap



object task1 {
    def main(args: Array[String]): Unit = {


        var spark = SparkSession.builder().appName("task1").getOrCreate()

        val sc = spark.sparkContext


        var rdd = sc.textFile(args(0))

        val jsonRdd = rdd.flatMap { line =>
                JSON.parseFull(line) match {
                    case Some(data: Map[String, Any]) => Some(data)
                    case _ => None  // Ignore invalid JSON lines
                }
            }


        // a
        var n_review = jsonRdd.map( _ => 1).reduce(_ + _)

        // b
        var n_review_2018 = jsonRdd.filter{ record =>
            record.get("date") match {
                case Some(date: String) => date.startsWith("2018")
                case _ => false
    
            }
        
        }.map( _ => 1).reduce(_ + _)


        // c
        var n_user = jsonRdd.map(record => record.get("user_id").getOrElse("").toString).distinct().count()

        // d
        val top10_user = jsonRdd.map(record => (record.get("user_id").getOrElse("").toString, 1)).reduceByKey(_ + _).sortBy{case (id, count) => (-count, id)}.take(10)
        val top10_user_list : List[List[Any]] = top10_user.map {case (id, count) => List(id, count)}.toList

        // e

        val n_business = jsonRdd.map(record => record.get("business_id").getOrElse("").toString).distinct().count()


        // f

        val top10_business = jsonRdd.map(record => (record.get("business_id").getOrElse("").toString, 1)).reduceByKey(_ + _).sortBy{ case (id, count) => (-count, id)}.take(10)
        val top10_business_list:List[List[Any]] = top10_business.map {case (id, count) => List(id, count)}.toList
    
    
        val outputPath = args(1)


        implicit val formats: Formats = Serialization.formats(NoTypeHints)


        val result: ListMap[String, Any] = ListMap(
            
            "n_review" -> n_review,
            "n_review_2018" -> n_review_2018,
            "n_user" -> n_user,
            "top10_user" -> top10_user_list,
            "n_business" -> n_business,
            "top10_business" -> top10_business_list

        )

        val jsonResult = write(result)


        val writer = new PrintWriter(outputPath)
        writer.write(jsonResult)
        writer.close()
    
        spark.stop()
    
    
    
    
    
    }
}