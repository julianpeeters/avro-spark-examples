import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.commons.lang.StringEscapeUtils.escapeCsv

import com.miguno.avro.twitter_schema

object WordCountJobAvroGenericSpark {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Avro Generic Spark Scala")//,
     // System.getenv().get("SPARK_HOME"), List("target/scala-2.10/avro-spark_2.10-1.0.jar"))

    val avroRdd = sc.newAPIHadoopFile("twitter.avro",
    								  classOf[AvroKeyInputFormat[GenericRecord]],
    								  classOf[AvroKey[GenericRecord]],
    								  classOf[NullWritable])

    val genericRecords = avroRdd.map{case (ak, _) => ak.datum()}

    val wordCounts = genericRecords.map((gr: GenericRecord) => gr.get("tweet").asInstanceOf[String])
    	.flatMap{tweet: String => tweet.split(" ")}
    	.map(word => (word, 1))
    	.reduceByKey((a, b) => a + b)

    val wordCountsFormatted = wordCounts.map{case (word, count) => (escapeCsv(word), count)}
    	.map{case (word, count) => s"$word,$count"}

  	wordCountsFormatted.saveAsTextFile("output/twitter-wordcount-scala-spark-generic.tsv")    
  }
}

object WordCountJobAvroSpecificSpark {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "Avro Specific Spark Scala")//,
   //   System.getenv().get("SPARK_HOME"), List("target/scala-2.10/avro-spark_2.10-1.0.jar"))

    val avroRdd = sc.newAPIHadoopFile("twitter.avro",
                      classOf[AvroKeyInputFormat[twitter_schema]],
                      classOf[AvroKey[twitter_schema]],
                      classOf[NullWritable])

    val specificRecords = avroRdd.map{case (ak, _) => ak.datum()}

    val wordCounts = specificRecords.map((sr: twitter_schema) => sr.get(1).asInstanceOf[String])
      .flatMap{tweet: String => tweet.split(" ")}
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

    val wordCountsFormatted = wordCounts.map{case (word, count) => (escapeCsv(word), count)}
      .map{case (word, count) => s"$word,$count"}

    wordCountsFormatted.saveAsTextFile("output/twitter-wordcount-scala-spark-specific.tsv")    
  }
}
