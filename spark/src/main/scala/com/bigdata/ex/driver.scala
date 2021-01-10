package com.bigdata.ex

object driver {

  def main(args: Array[String]): Unit = {

    /* Setting system properties and log level*/
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val process_obj = Class.forName("com.bigdata.ex.Sample").newInstance().asInstanceOf[com.bigdata.ex.Transformation]
    
    process_obj.initialize()
    process_obj.process()
    
  }
}