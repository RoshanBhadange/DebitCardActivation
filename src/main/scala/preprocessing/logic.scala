package preprocessing
import config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rank}
import utils.readHiveTables

object logic {
  def latestRecordPerCust(df: DataFrame): DataFrame ={
    val win = Window.partitionBy(col("account_no"),col("sort_code"),col("party_id"))
    val dfLatest = df.withColumn("rnk", rank() over(win).orderBy(col("ingestion_year"),col("ingestion_month"),col("ingestion_day").desc))
    dfLatest.where("rnk=1")
  }

  def process(): Unit = {

    val dispatchDf = readHiveTables.readTable(s"${Config.dispatchDb}", "dispatch", "edh_source_extract_ts")

    val dispFilter = dispatchDf.where("edh_ingest_delete_flag=='FALSE' and notif_sub_code =='2'")
    val latdispFilter = latestRecordPerCust(dispFilter)
    latdispFilter.show()

    val transdf = readHiveTables.readTable(s"${Config.transactionsDb}", "transactions", "trn_date")

    transdf.show()

  }

}
