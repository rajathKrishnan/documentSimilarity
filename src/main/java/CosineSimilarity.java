import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class CosineSimilarity {
    private static final Pattern SPACE = Pattern.compile(" ");

    // SMALL EXAMPLE
    private static final String DOC1_PATH = "/Users/RajKrishnan/git/DocumentSimilarity/src/main/resources/doc1.txt";
    private static final String DOC2_PATH = "/Users/RajKrishnan/git/DocumentSimilarity/src/main/resources/doc2.txt";
    private static final String QUERY1_PATH = "/Users/RajKrishnan/git/DocumentSimilarity/src/main/resources/query1.txt";

    // SHAKESPEARE EXAMPLE
    private static final String SHAKESPEARE1_PATH = "/Users/RajKrishnan/git/DocumentSimilarity/src/main/resources/shakespeare1.txt";
    private static final String SHAKESPEARE2_PATH = "/Users/RajKrishnan/git/DocumentSimilarity/src/main/resources/shakespeare2.txt";
    private static final String QUERY2_PATH = "/Users/RajKrishnan/git/DocumentSimilarity/src/main/resources/query2.txt";

    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Cosine Similarity");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = SparkSession.builder().sparkContext(javaSparkContext.sc()).getOrCreate();

        pageRankExample(sparkSession, DOC1_PATH, DOC2_PATH, QUERY1_PATH);
//        pageRankExample(sparkSession, SHAKESPEARE1_PATH, SHAKESPEARE2_PATH, QUERY2_PATH);
    }

    private static void pageRankExample(SparkSession sparkSession, String doc1, String doc2, String query) {
        List<String> results = new ArrayList<>();
        results.add("The similarity of doc1 and doc2 is : " + cosineSimilarity(sparkSession, doc1, doc2));
        results.add("The similarity of query and doc1 is : " + cosineSimilarity(sparkSession, query, doc1));
        results.add("The similarity of query and doc2 is : " + cosineSimilarity(sparkSession, query, doc2));
        results.forEach(System.out::println);
    }

    private static double cosineSimilarity(SparkSession sparkSession, String path1, String path2) {
        // read in file and store as RDD
        JavaRDD<String> lines1 = sparkSession.read().textFile(path1).javaRDD();
        JavaRDD<String> lines2 = sparkSession.read().textFile(path2).javaRDD();

        // map lines -> words
        JavaRDD<String> words1 = lines1.flatMap(s -> Arrays.asList(SPACE.split(s.toLowerCase())).iterator());
        JavaRDD<String> words2 = lines2.flatMap(s -> Arrays.asList(SPACE.split(s.toLowerCase())).iterator());

        // map words -> words + count (1 for now)
        JavaPairRDD<String, Integer> assignOnes1 = words1.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> assignOnes2 = words2.mapToPair(s -> new Tuple2<>(s, 1));

        // reduce words by key to get words + number of occurences
        JavaPairRDD<String, Integer> wordCounts1 = assignOnes1.reduceByKey((i1, i2) -> i1 + i2).sortByKey();
        JavaPairRDD<String, Integer> wordCounts2 = assignOnes2.reduceByKey((i1, i2) -> i1 + i2).sortByKey();

        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<Integer>>> outerJoinedRdd = wordCounts1.fullOuterJoin(wordCounts2);

        // creates RDD with word | (wordCount1, wordCount2)
        JavaPairRDD<String, Tuple2<Integer, Integer>> result = outerJoinedRdd.mapValues(tuple -> {
            if(!tuple._1().isPresent()) {
                return new Tuple2<>(0, tuple._2().get());
            } else if(!tuple._2().isPresent()) {
                return new Tuple2<>(tuple._1().get(), 0);
            } else {
                return new Tuple2<>(tuple._1().get(), tuple._2().get());
            }
        });

        // do dot product calculation
        JavaRDD<Tuple2<Integer, Integer>> resultTuple = result.values();
        JavaRDD<Integer> dotProduct = resultTuple.map(s -> s._1() * s._2());

        int sumDotProduct = dotProduct.collect().stream().mapToInt(Integer::intValue).sum();
        return cosineSimilarity(wordCounts1, wordCounts2, sumDotProduct);
    }

    private static double cosineSimilarity(JavaPairRDD<String, Integer> a, JavaPairRDD<String, Integer> b, int dotProduct) {
        return dotProduct / (calculateLength(a) * calculateLength(b));
    }

    private static double calculateLength(JavaPairRDD<String, Integer> rdd) {
        List<Integer> values = rdd.values().collect();
        int sum = 0;
        for(Integer i : values) {
            sum += Math.pow(i, i);
        }
        return Math.sqrt(sum);
    }

    private static <K, V> void printJavaPairRDD(JavaPairRDD<K, V> rdd) {
        rdd.foreach(entry -> System.out.println(entry._1 + "  |  "  + entry._2));
    }

    private static <K> void printJavaRDD(JavaRDD<K> rdd) {
        rdd.foreach(entry -> System.out.println(entry.toString()));
    }
}
