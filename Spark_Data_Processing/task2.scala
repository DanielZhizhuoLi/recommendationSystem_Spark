import org.apache.spark.sql.SparkSession
import scala.util.parsing.json.JSON
import java.io.PrintWriter
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import scala.collection.immutable.ListMap
import org.apache.spark.HashPartitioner






object task2 {
    def main(args: Array[String]): Unit = {
        
        val spark = SparkSession.builder.appName("task2").getOrCreate()
        val sc = spark.sparkContext

        val rdd = sc.textFile(args(0))
        val outputPath = args(1)
        val partition = args(2)

        val jsonRdd = rdd.flatMap{ line =>
            JSON.parseFull(line)
            match {
                case Some(data: Map[String, Any]) => Some(data)
                case _ => None
            }
        }

        // default
        val n_partition_d = jsonRdd.getNumPartitions

        val n_items_d = jsonRdd.mapPartitions( iter => Iterator(iter.length)).collect()

        val startTimeD = System.nanoTime()
        val top10_business_D = jsonRdd.map(line => (line.get("business_id").getOrElse("").toString, 1)).reduceByKey(_+_).sortBy{ case (id, number) => (-number, id) }.take(10)
        val exe_time_D = (System.nanoTime() - startTimeD) / 1e9


        // customized

        val customized_rdd = jsonRdd.map(line => (line.get("business_id").getOrElse("").toString, 1)).partitionBy(new HashPartitioner(partition.toInt))
        val n_partition_c = customized_rdd.getNumPartitions

        val n_items_c = customized_rdd.mapPartitions( iter => Iterator(iter.length)).collect()


        val startTimeC = System.nanoTime()
        val top10_business_C = customized_rdd.reduceByKey(_+_).sortBy{ case (id, number) => (-number, id) }.take(10)
        val exe_time_C = (System.nanoTime() - startTimeC) / 1e9


        implicit val formats: Formats = Serialization.formats(NoTypeHints)

        val result: ListMap[String, Any] = ListMap(
            "default" -> ListMap(
                "n_partition" -> n_partition_d,
                "n_items" -> n_items_d,
                "exe_time" -> exe_time_D
            ),
            "customized" -> ListMap(
                "n_partition" -> n_partition_c,
                "n_items" -> n_items_c,
                "exe_time" -> exe_time_C
            )
        )
        val jsonResult = write(result)


        val writer = new PrintWriter(outputPath)
        writer.write(jsonResult)
        writer.close()
    
        spark.stop()
    

    }

}