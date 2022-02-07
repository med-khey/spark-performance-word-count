import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object wordcount {

  def main(args: Array[String]): Unit = {
    val start1: Long = System.currentTimeMillis

    val conf: SparkConf = new SparkConf()
    .setAppName("Spark Performance Word Count")
    .setMaster("local[1]")
    val sc = new SparkContext(conf)
    val text_file: RDD[String] = sc.textFile("data/random_text.txt")
    val count: RDD[(String, Int)] = text_file.flatMap(line => line.split(" "))
                                              .map(word  => (word, 1))
                                              .reduceByKey(_+_)
                                              .sortBy(_._2)

    count.saveAsTextFile("data/output_scala")

    val end1: Long = System.currentTimeMillis
    System.out.println("Elapsed Time in mili seconds: " + (end1 - start1))


  }
}