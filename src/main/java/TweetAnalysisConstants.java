import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TweetAnalysisConstants {
    public static final String SOURCE = "source";
    public static final String TEXT = "text";
    public static final String CREATED_AT = "created_at";
    public static final String RETWEET_COUNT = "retweet_count";
    public static final String FAVORITE_COUNT = "favorite_count";
    public static final String IS_RETWEET = "is_retweet";
    public static final String ID_STR = "id_str";

    public static final StructType TWEET_SCHEMA = new StructType(
            new StructField[] {
                    new StructField(SOURCE, DataTypes.StringType, true, Metadata.empty()),
                    new StructField(TEXT, DataTypes.StringType, true, Metadata.empty()),
                    new StructField(CREATED_AT, DataTypes.DateType, true, Metadata.empty()),
                    new StructField(RETWEET_COUNT, DataTypes.LongType, true, Metadata.empty()),
                    new StructField(FAVORITE_COUNT, DataTypes.LongType, true, Metadata.empty()),
                    new StructField(IS_RETWEET, DataTypes.BooleanType, true, Metadata.empty()),
                    new StructField(ID_STR, DataTypes.StringType, true, Metadata.empty())
            }
    );
}
