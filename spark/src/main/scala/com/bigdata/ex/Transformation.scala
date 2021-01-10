package com.bigdata.ex


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame


trait Transformation {

      def initialize():SparkSession
      def process()

}
