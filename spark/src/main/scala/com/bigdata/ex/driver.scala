package com.bigdata.ex

object driver {

  def main(args: Array[String]): Unit = {

    /* Setting system properties */
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    
    /*
     * creating an object to intiate the process 
     * 
     * This sample project has two methods 
     * 1.Initialize --> This will create the spark session --> Entry point for spark execution 
     * 2.Process --> This will start the process and run the instructions 
     * 
     * we can use property file --> which will contain the process name , and class name to run we can read the property file
     * and parse the arguments and trigger the code for different data sources
     * 
     *  */

    val process_obj = Class.forName("com.bigdata.ex.Sample").newInstance().asInstanceOf[com.bigdata.ex.Transformation]
    
    /*
     * 
     * 
     * */
    
    process_obj.initialize()
    process_obj.process()
    
  }
}
