import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public class Application {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String DOC1_PATH = "/Users/RajKrishnan/git/DocumentSimilarity/src/main/resources/doc1.txt";
    private static final String DOC2_PATH = "/Users/RajKrishnan/git/DocumentSimilarity/src/main/resources/doc2.txt";

    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Document Similarity");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = SparkSession.builder().sparkContext(javaSparkContext.sc()).getOrCreate();
        cosineSimilarity(sparkSession);
    }

    private static void cosineSimilarity(SparkSession sparkSession) {
        JavaRDD<String> lines1 = sparkSession.read().textFile(DOC1_PATH).javaRDD();
        JavaRDD<String> lines2 = sparkSession.read().textFile(DOC2_PATH).javaRDD();

        JavaRDD<String> words1 = lines1.flatMap(s -> Arrays.asList(SPACE.split(s.toLowerCase())).iterator());
        JavaRDD<String> words2 = lines2.flatMap(s -> Arrays.asList(SPACE.split(s.toLowerCase())).iterator());

        JavaPairRDD<String, Integer> assignOnes1 = words1.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> assignOnes2 = words2.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> wordCounts1 = assignOnes1.reduceByKey((i1, i2) -> i1 + i2).sortByKey();
        JavaPairRDD<String, Integer> wordCounts2 = assignOnes2.reduceByKey((i1, i2) -> i1 + i2).sortByKey();

        printJavaPairRDD(wordCounts1);

        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<Integer>>> rdd = wordCounts1.fullOuterJoin(wordCounts2);
        printJavaPairRDD(rdd);


//        JavaPairRDD<String, Tuple2<Integer, Integer>> pls = rdd.mapToPair(tuple -> {
//            if(tuple._2()._1().isPresent()) {
//                return new Tuple2<>(tuple._2()._1().get(), tuple._2()._2().get());
//            } else {
//                return new Tuple2<>(0, tuple._2()._2().get());
//            }
//        });

        printJavaPairRDD(rdd);

    }

    private static <K, V> void printJavaPairRDD(JavaPairRDD<K, V> rdd) {
        rdd.foreach(entry -> System.out.println(entry._1 + "  |  "  + entry._2));
    }
}
