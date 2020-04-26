import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}


object MyMain {

  case class rule(ServiceId: String, ServiceDesc: String, StartTime: Int, EndTime: Int, TotalVol: Int)
  case class data (MSISDN : String , ServiceId: String , TrafficVol:Int , Time: Double)
  case class segment(MSISDN: String)

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val conf = new SparkConf()
    conf.setMaster("local") //local machine
    conf.setAppName(" Scala Project ")
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    sc.setLogLevel("ERROR")


    val Data_Schema = StructType(
      Array(
        StructField("MSISDN", IntegerType, true),
        StructField("ServiceId", StringType, true),
        StructField("TrafficVol", IntegerType, true),
        StructField("Time", LongType, true)))

    if (true) { // args.length > 3
      val data_path: String = "E:\\data_sets\\data" // args(0)
      val rules_path: String = "E:\\data_sets\\RULES.csv" // args(1)
      val segments_path: String = "E:\\data_sets\\SEGMENT.csv" // args(2)

      var Data = sqlContext.createDataFrame(sc.emptyRDD[Row], Data_Schema)
      try {
        Data = sqlContext.read.option("header", "false").schema(Data_Schema).csv(data_path)
      } catch {
        case ex: AnalysisException => {
          println("Error: Path does not exist!")
          System.exit(1)
        }
        case unknown: Exception => {
          println(s"Unknown exception: $unknown")
          System.exit(1)
        }
      }

      //Reading rules Csv file
      var df_rules = sqlContext.createDataFrame(sc.emptyRDD[Row], Data_Schema)
      try {
        val rdd_rules = sc.textFile(rules_path)
        println("File RULES.csv Read Successfully")
        df_rules = rdd_rules.map {
          line =>
            val column = line.split(",")
            rule(column(0), column(1), column(2).toInt, column(3).toInt, column(4).toInt)
        }.toDF()
      } catch {
        case _: Throwable => {
          println("Error: Input path does not exist!")
          System.exit(1)
        }
        case unknown: Exception => {
          println(s"Unknown exception: $unknown")
          System.exit(1)
        }
      }

      //Reading Segment Csv file
      var df_segment = sqlContext.createDataFrame(sc.emptyRDD[Row], Data_Schema)
      try {
        val rdd_segment = sc.textFile(segments_path)
        println("File SEGMENT.csv Read Successfully")
        df_segment = rdd_segment.map {
          line =>
            val column = line.split(",")
            segment(column(0))
        }.toDF()
      } catch {
        case _: Throwable => {
          println("Error: Input path does not exist!")
          System.exit(1)
        }
        case unknown: Exception => {
          println(s"Unknown exception: $unknown")
          System.exit(1)
        }
      }

      // Join segments with data
      val df_data_segment = Data.join(df_segment, "MSISDN")

      // Join (segments and data) with rules, where TrafficVol greater than TotalVol
      val df_output = df_data_segment.join(df_rules, "ServiceId").filter($"TrafficVol" > $"TotalVol").coalesce(1)

      // Filter data with older timestamp for the segments and services
      val grouped_df = df_output.groupBy("MSISDN", "ServiceId").min("TrafficVol", "Time")

      // Write the output
      grouped_df.write.mode("append").csv("C:\\ITI_Sala_Project\\Output")
      println("Data frame csv file saved successfully!")

    } else {
      println("Error: Some arguments are missing!")
      System.exit(1)
    }
  }
}
