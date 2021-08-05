import org.apache.spark.sql.types._
import org.apache.spark.sql.finctions._
import org.apache.spark.sql.SparkSession


object Test extends App {

    val spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("test")
        .enableHiveSupport()
        .getOrCreate()

    val schema = new StructType()
        .add("durationMs", Datetime)
        .add("position", LongType)
        .add("owners", LongType)
        .add("resources", StringType)

    val df = spark.read.schema(schema).json("../data/feeds_show.json")

    // validation schema
    def validateDfSchema(): Boolean = {
        val requiredSchema = StructType(
            List(
                StructField("durationMs", Datetime, true),
                StructField("position", LongType, true),
                StructField("owners", LongType, true),
                StructField("resources", StringType, true)
            )
        )
        validateSchema(df, requiredSchema)
    }

    // comparison with expected DataFrame
    val expectedDF: DataFrame = spark.read.json("path_to_validation_dataframe")
    def comparisonDFs(expectedDF: DataFrame, currentDF: DataFrame, delta: Double): Boolean = {
        import spark.implicits._
        val countTrues = expectedDF.join(currentDF, Seq($"owners", $"resources"))
            .count()

        countTrues >= delta
    }

    // chech Nulls column
    def checkNullColumn(df: DataFrame, col: Column): Boolean = {
        df.filter(col.isNotNull).select(col).isEmpty
    }
}
