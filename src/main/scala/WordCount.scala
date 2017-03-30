import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._

/**
  * Created by suresh on 29/03/17.
  */
object WordCount {


  def main(args: Array[String])={

    //Create Streaming context
    var conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(10))

    //establish socket connection
    val lines = KafkaUtils.createStream(ssc,"localhost:2181"  ,"spark-streaming-consumer-group", Map("karpital"->3) )

    //Split lines by " " (space)
    val words = lines.flatMap(_._2.split(" "))
    val pairs = words.map( x => (x,1))
    val wordcnt = pairs.reduceByKey(_ + _) // count words present in the map

    wordcnt.print()

    ssc.start() // start the computation
    ssc.awaitTermination() // Wait for computation to terminate




  }
}
