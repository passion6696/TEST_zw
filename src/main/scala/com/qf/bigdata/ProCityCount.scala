package com.qf.bigdata

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 统计各个省市的数据量
 */
object ProCityCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","E:\\hadoop-2.7.6")
    val spark = SparkSession.builder()
      .appName("test")
      .master("local")
      .getOrCreate()

    // 导入内置函数
    import org.apache.spark.sql.functions._
    // 读取数据
    val df = spark.read.load("output/rs")
    // 按照省市分组求Count
//    df.groupBy("provincename","cityname")
//      .count()
//      .show()
    df.createTempView("log")
    val sql=
      """
        |select
        |provincename,cityname,count(1) as counts
        |from
        |log
        |group by
        | provincename,cityname
        |""".stripMargin
    // spark.sql("select provincename,cityname,count(1) as counts from log group by provincename,cityname")
     val frame: DataFrame = spark.sql(sql)
    frame
//      .coalesce(1)    写到文件中
//      .write
//      .mode(SaveMode.Overwrite) //写入
//      .partitionBy("provincename","cityname") //分区
//      .json("city/rs") //路径

    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123456")
    //这里如果tt库里没有log表 是会自己创建的
    frame.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/tt", "log",prop)
     // .write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306","tt.log",)


  }
}
