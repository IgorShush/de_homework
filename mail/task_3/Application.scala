import org.apache.spark.sql.types._
import org.apache.spark.sql.finctions._
import org.apache.spark.sql.SparkSession


object Application extends App {
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

    // 1
    // O(n/numPartitions)
    val partitionedDF = df.where('time_key === current_date).cache()
    val countPlatformUsers = partitionedDF.groupBy('platform).agg(count('users).alias("countPlatformUsers"))
    val countUsers = countPlatformUsers.groupBy('platform).agg(sum('countPlatformUsers).alias("countUsers"))

    // 2
    // O(n)
    val partitionedDF = df.where('time_key === current_date).cache()
    val countAuthors = partitionedDF.groupBy('owners).count()
    val count = df.groupBy('resources).count()

    // 3 O(nlogn) cause of lag function
    val partitionedDF = df.where('time_key === current_date).cache()
    val countSessions = partitionedDF
        .withColumn("is_current_session_alive", when('timestamp + 'duration > lag('timestamp).over(partitionBy('userId), 1).otherwise(0)))
        .where('is_current_session_alive === 0)
        .groupBy('userId, 'platform)
        .agg(count('is_current_session_alive).alias("countSessions"), mean('duration).alias("mean_duration"),
        mean('position).alias("mean_position"))


    // 4 O(n)
    val countUserFeeds = df.join(broadcast(users), Seq('userId))
        .groupBy('userId).count()
}