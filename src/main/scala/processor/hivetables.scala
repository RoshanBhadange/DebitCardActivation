package processor
import utils.createtables
import utils.readHiveTables.spark
import config.Config
import org.apache.spark.sql.functions.{col, current_timestamp, to_timestamp}

object hivetables {

  def createDatabase(): Unit ={
    spark.sql("CREATE DATABASE IF NOT EXISTS schema_cms")
    spark.sql("CREATE DATABASE IF NOT EXISTS hive_fcm")
    spark.sql("CREATE DATABASE IF NOT EXISTS prod_hive_pas")
  }

  def createHiveTables() {
    spark.sql("USE schema_cms;")
    spark.sql(createtables.stg_dispatch)
    spark.sql("USE hive_fcm;")
    spark.sql(createtables.stg_transactions)
    spark.sql("USE prod_hive_pas;")
    spark.sql(createtables.debit_card_activation_insights)
  }

  def load_table_disp(): Unit ={
    val df = spark.read.option("delimiter",",").option("header",true).csv("src/main/resources/data/dispatch_table.csv")
    val dfdisp = df.withColumn("event_ts",col("event_ts").cast("date"))
      .withColumn("edh_ingest_ts",col("edh_ingest_ts").cast("date"))

    dfdisp.write.mode("overwrite").insertInto(s"${Config.dispatchDb}.${Config.dispatchtbl}")
  }
  def load_tbl_trans(): Unit ={
    val df = spark.read.option("delimiter",",").option("header",true).csv("src/main/resources/data/transactions_table.csv")
    df.write.mode("overwrite").insertInto(s"${Config.transactionsDb}.${Config.transtbl}")
  }

  def dropTable(): Unit = {
    spark.sql("DROP TABLE schema_cms.dispatch")
    spark.sql("DROP TABLE hive_fcm.transactions")
    spark.sql("DROP TABLE prod_hive_pas.debit_card_activation_insights")

  }

}