import org.apache.spark.sql.SparkSession
import scala.util.parsing.json.JSON
import java.io.PrintWriter
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.{Set, Map}
import scala.util.control.Breaks._



object task1 {


    def apriori(partitiondata: List[Set[String]], support: Float): List[Set[String]] = {

        var itemCount: mutable.Map[String, Int] = mutable.Map().withDefaultValue(0)

        partitiondata.foreach{ basket => 
            basket.foreach{
                item => itemCount(item) += 1
            }
        }

        val L1: Set[Set[String]] = itemCount.collect {
            case (item, count) if count >= support => Set(item)
        }.toSet


        var frequentItemset = mutable.ListBuffer[Set[String]]()
        frequentItemset.appendAll(L1)

        var k:Int = 2  
        var current_Lk = L1

        while (current_Lk.nonEmpty) {
            // faltten
            val uniqueItems = current_Lk.flatten

            val Ck = uniqueItems.subsets(k)
                .filter(candidate => candidate.subsets(k - 1).forall(current_Lk
                .contains))
                .map(_.toSet) // Convert to immutable Set
                .toSet

            val itemsetCount = mutable.Map[Set[String], Int]().withDefaultValue(0)
            
            for (basket <- partitiondata) {
                for (itemset <- Ck) {
                    if (itemset.subsetOf(basket)) {
                        itemsetCount(itemset) += 1
                    }
                }
            }
            
            val Lk = itemsetCount.filter { case (_, count) => count >= support }.keySet

            if (Lk.isEmpty) return frequentItemset

            frequentItemset ++= Lk
            current_Lk = Lk
            k += 1

        }


        frequentItemset.toList


    }


















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