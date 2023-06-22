package utils
import org.apache.spark.sql.{DataFrame, SparkSession}
import config.Config

object readHiveTables {
  val spark = SparkSession.builder()
    .master("local")
    .enableHiveSupport()
    .appName("DebitCardActivation")
    .config("hive.exec.dynamic.partition",true)
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .getOrCreate()

  def readTable(Database: String,TableName: String, run_date: String): DataFrame ={
    val df = spark.sql(s"select * from ${Database}.$TableName where concat(SUBSTR($run_date,7,4),'-',SUBSTR($run_date,4,2),'-',SUBSTR($run_date,0,2)) >= date_sub(current_date(), 45)")
    df
  }
}
