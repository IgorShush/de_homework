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

        // Сразу с фильтром, чтобы уменьшить объем хранимого датафрейма в памяти
        val payments = spark.table("payments")
            .where($"event_dt" >= date_add(current_date(), -30))
        val games = spark.table("games")

        // Вычисляем среднее по платформе. Выносим в отдельный val, чтобы не пересчитывать каждый раз
        val avg_results = games.alias("g")
            .join(payments.alias("p"), $"g.game_id" === $"p.game_id", "left")
            .groupBy($"g.platform")
            .agg(sum($"p.payment_sum").alias("sum_all"), count($"p.player_id").alias("count_all"))
            .withColumn("avg_platform", $"sum_all" / $"count_all")
            .select($"g.platform", $"sum_all", $"count_all", )

        val res = payments.alias("p")
            .join(games.alias("g"), $"g.game_id" === $"p.game_id", "left")
            .join(avg_results.alias("avg"), $"avg.platform" = $"g.platform", "left")
            .withColumn("payments_sum", sum($"p.payment_sum").over(partitionBy($"p.game_id", $"g.platform")))
            .withColumn("players_count", count($"p.player_id").over(partitionBy($"p.game_id", $"g.platform")))
            .withColumn("arppu", $"payments_sum" / $"players_count")
            .select(
                $"g.game_name",
                $"arppu",
                when($"arppu" > $"avg.avg_platform", lit(1)).otherwise(lit(0)).alias("is_above_platform_avg")
            ).distinct()
        
        res
            .write
            .mode(SaveMode.Overwrite)
            .format("parquet")
            .saveAsTable("above_platform_payments")  

        spark.stop()
    }
}