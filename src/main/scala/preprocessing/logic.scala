package preprocessing
import config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, current_date, lit, rank, substring, when}
import utils.readHiveTables

object logic {
  def latestRecordPerCust(df: DataFrame): DataFrame ={
    val win = Window.partitionBy(col("account_no"),col("sort_code"),col("party_id"))
    val dfLatest = df.withColumn("rnk", rank() over(win).orderBy(col("ingestion_year"),col("ingestion_month"),col("ingestion_day").desc))
    val latestRecordDf = dfLatest.where("rnk=1")
    latestRecordDf
  }

  def process(): Unit = {

    val dispatchDf = readHiveTables.readTable(s"${Config.dispatchDb}", "dispatch", "edh_source_extract_ts")

    val dispFilter = dispatchDf.where("edh_ingest_delete_flag=='FALSE' and notif_sub_code =='2'")
    val dispatch = latestRecordPerCust(dispFilter)
    dispatch.show()
    val transdf = readHiveTables.readTable(s"${Config.transactionsDb}", "transactions", "trn_date")
    val transFilter = transdf.filter("(txn_code=='326' and txn_class='13') or (txn_code=='981' and txn_class='1')")
    val transactions = transFilter.toDF()
    // RCA
//     dispatch.select(col("account_no"),col("sort_code"),col("pan"),substring(col("pan"),-4,4))
//      .filter("account_no=='12345672' and sort_code=='121209'").show()
//     transactions.select(col("cbs_acc_num"),col("acc_srt_cd"),col("crd_num_issue_num"),substring(col("crd_num_issue_num"),-4,4))
//      .filter("cbs_acc_num=='12345672' and acc_srt_cd=='121209'").show()
    val dfJoin = dispatch.join(transactions,col("account_no") === col("cbs_acc_num")
      && col("sort_code") === col("acc_srt_cd")
      && substring(col("pan"),-4,4)===substring(col("crd_num_issue_num"),-4,4)
    )
    val finaldf = dfJoin.filter("trn_date is not null")
    val targetdf = finaldf.withColumn("reminder_group", when(col("ingestion_day") between(1,15), "R1").when(col("ingestion_day") between(16,30),"R2").otherwise("R3"))
      .withColumnRenamed("account_no", "acct_no")
      .withColumnRenamed("edh_source_extract_ts", "card_disptach_date")
      .withColumn("pipeline_execution_date", current_date())
      .withColumn("brand", lit("BOS"))
      .select("acct_no","sort_code","pan","party_id","brand","reminder_group","card_disptach_date","pipeline_execution_date")

    targetdf.show()
    targetdf.write.mode("overwrite").insertInto(s"${Config.targetDb}.${Config.targettbl}")
  }
}
