package com.bigdata.ex

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import com.bigdata.ex.Transformation
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

class Sample extends Transformation {

  /**
   * Sample Demonstration
   * Author :
   * Date : 10/01/2020
   */

  var spark: SparkSession = _


  override def initialize(): SparkSession =
    {
      spark = SparkSession.builder().appName("Sample Demonstration").master("local").getOrCreate()
      return spark
    }

  override def process() = {

    val emp_df1 = spark.
      read.
      format("csv").
      option("inferSchema", true).
      option("header", true).
      load("C:\\Users\\vishn\\Desktop\\Hadoop\\Employee")

    val dept_df1 = spark.
      read.
      format("csv").
      option("inferSchema", true).
      option("header", true).
      load("C:\\Users\\vishn\\Desktop\\Hadoop\\Departments")
      
      /*By Default sort merge joins will take place
       * 
       * Since the two datasets are very small and broadcast hash join will takes place.
       * 
       * if one of the dataset is small we can broadcast the cast and perfrom the join which will be 
       * faster when compared to sort merge join
       * 
       * 
       * */

    val joined_df = emp_df1.as("emp").join(dept_df1.as("dept"), col("emp.DEPARTMENT_ID") === col("dept.DEPARTMENT_ID"), "inner")

    def concat_udf(first: String, last: String): String = {

      var full_name = first.concat(" ").concat(last)

      full_name

    }

    val concat_udf1 = udf(concat_udf _)

    val final_df = joined_df.withColumn("Full_name", concat_udf1(col("FIRST_NAME"), col("LAST_NAME")))
    
    /* using the show function to execute the process
     * as the spark executes lazily
     * 
     * 
     *  */
    

    final_df.show(10)
  }

}
  
