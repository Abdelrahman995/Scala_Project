import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import scala.io.Source
import java.io.File

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import scala.reflect.io.File
import scala.collection.mutable.ListBuffer

object MyMain {

  case class rule(ServiceId: String, ServiceDesc: String, StartTime: Int, EndTime: Int, TotalVol: Int)
  case class data (MSISDN : String , ServiceId: String , TrafficVol:Int , Time: Double)
  case class segment(MSISDN: String)

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val conf = new SparkConf()
    conf.setMaster("local") //local machine
    conf.setAppName(" Scala Final Project ")
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

    val Data = sqlContext.read.option("header", "false").schema(Data_Schema).csv("D:\\ITI\\Scala\\Data")
    println(" ===== ALL DATA FILES  =====")
    Data.write.mode("append").csv("D:\\ITI\\Scala\\data_Generated")

    //Reading rules Csv file
    val rdd_rules = sc.textFile("D:\\ITI\\Scala\\RULES.csv")
    println("File RULES.csv Read Successfully")
    val df_rules = rdd_rules.map {
      line =>
        val column = line.split(",")
        rule(column(0), column(1), column(2).toInt, column(3).toInt,column(4).toInt)
    }.toDF()
    //df_rules.show()

    //Reading Segment Csv file
    val rdd_segment = sc.textFile("D:\\ITI\\Scala\\SEGMENT.csv")
    println("File SEGMENT.csv Read Successfully")
    val df_segment = rdd_segment.map {
      line =>
        val column = line.split(",")
        segment(column(0))
    }.toDF()
    //df_segment.show()


      // Filteration of Data on Segment numbers
      val df_data_segment = Data.join(df_segment, "MSISDN")
      //println(" Data and Segment joined data frame output :- ")
      //df_data_segment.show()
      //println(" COUNT OF DATA JOINED WITH SEGMENT :- "+df_data_segment.count())

      println(" data frame output :- ")
      val df_output = df_data_segment.join(df_rules, "ServiceId").filter($"TrafficVol" > $"TotalVol").coalesce(1)
      // println(" COUNT OF DATA JOINED WITH SEGMENT After Filter  :- "+df_output.count())

      val grouped_df = df_output.groupBy("MSISDN","ServiceId").min("TrafficVol","Time")
      println(" COUNT OF DATA JOINED WITH SEGMENT After Filter  :- "+grouped_df.count())
      grouped_df.show()

      //val final_output =  grouped_df.join(df_output, grouped_df("ServiceId") === df_output("ServiceId") && grouped_df("MSISDN") === df_output("MSISDN"),"left")
      //println(Data_Files)
      //var x = a.concat("OUTPUT")
    grouped_df.write.mode("append").csv("D:\\ITI\\Scala\\Success")

      println(" data frame csv file saved successfully .. ")
  }
}


/*
    val df_data = rdd_data.map(line => line.split(",")).map(fields => (fields(1) , (fields(0),fields(2),fields(3))))
    /*println("Pair RDD data")
    pair_data.foreach(f=>{
      println(f)
    })*/

    val pair_rules = rdd_rules.map(line => line.split(",")).map(fields => (fields(0) , (fields(1),fields(2),fields(3),fields(4))))
    /*println("Pair RDD Rules")
    pair_rules.foreach(f=>{
      println(f)
    })*/

    val dataDF = rdd_data.map {
      line =>
        val column = line.split(",")
        data(column(0), column(1), column(2).toInt, column(3).toDouble)
    }.toDF()

    dataDF.show()

    /*val joined_rdd = pair_data.join(pair_rules)
    println("============  JOIN RDDS  ============")
    joined_rdd.foreach(f=>{
      println(f)
    })*/
 */

