import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

public class TweetAnalysis {
    private static final String TWEET_PATH = "/Users/RajKrishnan/git/DocumentSimilarity/src/main/resources/tweets.csv";

    public static void main(String args[]) throws AnalysisException {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Cosine Similarity");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = SparkSession.builder().sparkContext(javaSparkContext.sc()).getOrCreate();
        SQLContext sqlContext = sparkSession.sqlContext();

        Dataset tweetSet = readCsv(TWEET_PATH, sqlContext, TweetAnalysisConstants.TWEET_SCHEMA);

        favoriteDevices(tweetSet);
//        tweetSet.where(col("text").contains("Obama")).show(10, false);
    }

    // Example of using "string SQL"
    private static void favoriteDevices(Dataset dataset) throws AnalysisException {
        SQLContext sqlContext = dataset.sqlContext();
        dataset.createTempView("tempView");

        Dataset devices = sqlContext.sql("SELECT source, COUNT(*) FROM tempView GROUP BY source ORDER BY COUNT(*) DESC");
        devices.show(50, false);
    }

    private static Dataset readCsv(String filePath, SQLContext sqlContext, StructType customSchema) {
        DataFrameReader reader = sqlContext.read().format("com.databricks.spark.csv").schema(customSchema);
        reader.option("inferSchema", "false");
        reader.option("header", "false");
        return reader.load(filePath);
    }
}
