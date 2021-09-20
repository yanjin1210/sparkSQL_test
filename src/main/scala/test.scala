import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import scala.math.random

object test {

  val sq = new ListBuffer[(String, String, Int)]()

  def randInt(n: Int): Int = (random * n).toInt

  def add_records(shopNum: Int, productNum: Int, amount: Int, num: Long): Unit = {
    for(i <- 0L until num) sq.append((randInt(shopNum).toString, randInt(productNum).toString, 1 + randInt(amount)))
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark Hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    spark.sql("create database if not exists test")
    spark.sql("use test")
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("select _1 as shop_id, count(_2) as transactions from sales group by _1 order by products desc").show()


//    val df = spark.sql("select * from sales")
//            .withColumnRenamed("_1", "shop_id")
//            .withColumnRenamed("_2", "product_id")
//            .withColumnRenamed("_3", "amount")
//    df.write.mode("overwrite").partitionBy("shop_id", "product_id").saveAsTable("sales_part")
//    add_records(10000, 100000, 5, 2000000L)
//    val df = sq.toDF()
//    df.write.mode("overwrite").saveAsTable("sales")
//    sq.clear()
//    for(i<-0 to 80) {
//      add_records(5, 200, 5, 2000000L)
//      val df = sq.toDF()
//      df.write.insertInto("sales")
//      sq.clear()
//    }
//    for(i<-0 to 240) {
//      add_records(5, 50, 5, 2000000L)
//      val df = sq.toDF()
//      df.write.insertInto("sales")
//      sq.clear()
//    }

    //df.write.mode("overwrite").saveAsTable("sales")
    spark.close()
  }
}
