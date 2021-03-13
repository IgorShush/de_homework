import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
org.apache.spark.sql.expressions.Window


object Application {
    def main(args: Array[String]): Unit = {
        
        val spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("test_app")
            .enableHiveSupport()
            .getOrCreate()
            
        import spark.implicits._

        val payments = spark.table("payments")
            .where($"event_dt" >= date_add(current_date(), -30))
        val games = spark.table("games")

        val res = games.alias("g")
            .join(payments.alias("p"), $"g.game_id" === $"p.game_id")
            .withColumn("payments_sum", sum($"p.payment_sum").over(partitionBy($"p.game_id")))
            .withColumn("players_count", count($"p.player_id").over(partitionBy($"p.game_id")))
            .withColumn("sum_all", sum($"p.payment_sum").over(partitionBy($"g.platform")))
            .withColumn("count_all", count($"p.player_id").over(partitionBy($"g.platform")))
            .withColumn("arppu", $"payments_sum" / $"players_count")
            .withColumn("avg_platform", $"sum_all" / $"count_all")
            .select(
                $"g.game_name",
                $"arppu",
                when($"arppu" > $"avg_platform", lit(1)).otherwise(lit(0)).alias("is_above_platform_avg")
            )
        
        res
            .write
            .mode(SaveMode.Overwrite)
            .format("parquet")
            .saveAsTable("above_platform_payments")  

        spark.stop()
    }
}