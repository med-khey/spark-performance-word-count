import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        long start1 = System.currentTimeMillis();

        SparkConf conf = new SparkConf()
                .setAppName("Spark Vs Python")
                .setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> text_file = sc.textFile("data/random_text.txt");
        JavaPairRDD<Object, Integer> count = text_file
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<Object, Integer>(word, 1))
                .reduceByKey(Integer::sum);
        count.saveAsTextFile("src/output_java");


        long end1 = System.currentTimeMillis();
        System.out.println("Elapsed Time in mili seconds: "+ (end1-start1));

    }
}
