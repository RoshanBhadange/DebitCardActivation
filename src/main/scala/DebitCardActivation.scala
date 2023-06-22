import processor.hivetables
import utils.readHiveTables
import utils.readHiveTables.spark
import config.Config
import preprocessing.logic

object DebitCardActivation extends App{
  // Main Script
  // Create Empty Tables\
  hivetables.dropTable()
  hivetables.createDatabase()
  hivetables.createHiveTables()
  hivetables.load_table_disp()
  hivetables.load_tbl_trans()
  logic.process()
  // logic
//  spark.sql("use show tables")

}
