import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.DriverManager

/**
 * @Author: huangJunJie  2020-09-28 19:01        
 */
object ParkingOfflineStatistics {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("ParkingOfflineStatistics")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    Class.forName("com.mysql.jdbc.Driver")

    import spark.implicits._
    import spark.sql

    //TODO SHELL 入参日期，如 2020-09-29
    val dateTime: String = args(0)


    val df: DataFrame = spark.read.table("my.my_ods_access_parking_lot")

    //    val rdd1: RDD[Row] = df.rdd
    //    rdd1.take(3).foreach(println)

    val rdd1: RDD[String] = df.toJSON.rdd

    val rdd2: RDD[(String, String, Integer, String)] = rdd1.map(line => {
      val obj: JSONObject = JSON.parseObject(line)
      val park_id: String = obj.getString("park_id")
      val enter_time: String = obj.getString("enter_time")
      val leave_time: String = obj.getString("leave_time")
      val is_left: Integer = obj.getInteger("is_left")
      (park_id, enter_time, is_left, leave_time)
    })

    val yesterdayData: RDD[(String, String, Integer, String)] = rdd2.filter(line => {
      line._2.contains(dateTime)
    })

//    val acquireHourTime: RDD[(String, String, Integer, String)] = yesterdayData.map(line => {
//      val hourEnterTime: String = line._2.substring(11, 13)
//      var hourLeaveTime: String = null
//      if (line._4 != null) {
//        hourLeaveTime = line._4.substring(11, 13)
//      }
//      (line._1, hourEnterTime, line._3, hourLeaveTime)
//    })

    //车辆进场数据
    val enterData: RDD[(String, String, Integer, String)] = yesterdayData.filter(line => {
      line._3 == 0
    })
    //截取车辆进场时间中的小时
    val hourEnterTime: RDD[(String, String, Integer, String)] = enterData.map(line => {
      val hourEnterTime: String = line._2.substring(11, 13)
      (line._1, hourEnterTime, line._3, line._4)
    })

    //车辆出场数据
    val leaveData: RDD[(String, String, Integer, String)] = yesterdayData.filter(line => {
      line._3 == 1
    })
    //截取车辆出场时间中的小时
    val hourLeaveTime: RDD[(String, String, Integer, String)] = leaveData.map(line => {
      val hourLeaveTime: String = line._4.substring(11, 13)
      (line._1, line._2, line._3, hourLeaveTime)
    })

    //车辆进场数据处理统计、统计结果入库
    val enterDataGroupByParkID: RDD[(String, Iterable[(String, String, Integer, String)])] = hourEnterTime.groupBy(_._1)
    val resultForEnterData: RDD[(String, String, Int, (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int), (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int), Int)] = enterDataGroupByParkID.map(line => {
      val list: List[(String, String, Integer, String)] = line._2.toList
      val total: Int = list.length
      var time_1: Int = 0
      var time_2: Int = 0
      var time_3: Int = 0
      var time_4: Int = 0
      var time_5: Int = 0
      var time_6: Int = 0
      var time_7: Int = 0
      var time_8: Int = 0
      var time_9: Int = 0
      var time_10: Int = 0
      var time_11: Int = 0
      var time_12: Int = 0
      var time_13: Int = 0
      var time_14: Int = 0
      var time_15: Int = 0
      var time_16: Int = 0
      var time_17: Int = 0
      var time_18: Int = 0
      var time_19: Int = 0
      var time_20: Int = 0
      var time_21: Int = 0
      var time_22: Int = 0
      var time_23: Int = 0
      var time_24: Int = 0
      for (i <- list) {
        val hour = i._2
        if (hour == "00")
          time_1 = time_1 + 1
        else if (hour == "01")
          time_2 = time_2 + 1
        else if (hour == "02")
          time_3 = time_3 + 1
        else if (hour == "03")
          time_4 = time_4 + 1
        else if (hour == "04")
          time_5 = time_5 + 1
        else if (hour == "05")
          time_6 = time_6 + 1
        else if (hour == "06")
          time_7 = time_7 + 1
        else if (hour == "07")
          time_8 = time_8 + 1
        else if (hour == "08")
          time_9 = time_9 + 1
        else if (hour == "09")
          time_10 = time_10 + 1
        else if (hour == "10")
          time_11 = time_11 + 1
        else if (hour == "11")
          time_12 = time_12 + 1
        else if (hour == "12")
          time_13 = time_13 + 1
        else if (hour == "13")
          time_14 = time_14 + 1
        else if (hour == "14")
          time_15 = time_15 + 1
        else if (hour == "15")
          time_16 = time_16 + 1
        else if (hour == "16")
          time_17 = time_17 + 1
        else if (hour == "17")
          time_18 = time_18 + 1
        else if (hour == "18")
          time_19 = time_19 + 1
        else if (hour == "19")
          time_20 = time_20 + 1
        else if (hour == "20")
          time_21 = time_21 + 1
        else if (hour == "21")
          time_22 = time_22 + 1
        else if (hour == "22")
          time_23 = time_23 + 1
        else if (hour == "23")
          time_24 = time_24 + 1
      }

      //元组中元素个数不能超过22个，将24个time装入两个元组中
      val tuple1 = Tuple12(time_1, time_2, time_3, time_4, time_5, time_6, time_7, time_8, time_9, time_10, time_11, time_12)
      val tuple2 = Tuple12(time_13, time_14, time_15, time_16, time_17, time_18, time_19, time_20, time_21, time_22, time_23, time_24)

      (line._1, dateTime, 0, tuple1, tuple2, total)
    })

    resultForEnterData.foreachPartition(iter => {
      val conn = DriverManager.getConnection("jdbc:mysql://192.168.1.220:3307/parking?useUnicode=true&characterEncoding=utf8", "parking", "parking@123")
      conn.setAutoCommit(false)
      val preparedStatement = conn.prepareStatement("insert into parking.parkinglot_offline_statistics (park_id,date,access_type,time_1,time_2,time_3,time_4,time_5,time_6,time_7,time_8,time_9,time_10,time_11,time_12,time_13,time_14,time_15,time_16,time_17,time_18,time_19,time_20,time_21,time_22,time_23,time_24,total) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);")
      iter.foreach(t => {
        preparedStatement.setString(1, t._1)
        preparedStatement.setString(2, t._2)
        preparedStatement.setInt(3, t._3)

        preparedStatement.setInt(4, t._4._1)
        preparedStatement.setInt(5, t._4._2)
        preparedStatement.setInt(6, t._4._3)
        preparedStatement.setInt(7, t._4._4)
        preparedStatement.setInt(8, t._4._5)
        preparedStatement.setInt(9, t._4._6)
        preparedStatement.setInt(10, t._4._7)
        preparedStatement.setInt(11, t._4._8)
        preparedStatement.setInt(12, t._4._9)
        preparedStatement.setInt(13, t._4._10)
        preparedStatement.setInt(14, t._4._11)
        preparedStatement.setInt(15, t._4._12)

        preparedStatement.setInt(16, t._5._1)
        preparedStatement.setInt(17, t._5._2)
        preparedStatement.setInt(18, t._5._3)
        preparedStatement.setInt(19, t._5._4)
        preparedStatement.setInt(20, t._5._5)
        preparedStatement.setInt(21, t._5._6)
        preparedStatement.setInt(22, t._5._7)
        preparedStatement.setInt(23, t._5._8)
        preparedStatement.setInt(24, t._5._9)
        preparedStatement.setInt(25, t._5._10)
        preparedStatement.setInt(26, t._5._11)
        preparedStatement.setInt(27, t._5._12)

        preparedStatement.setInt(28, t._6)

        preparedStatement.addBatch()
      })
      preparedStatement.executeBatch()
      conn.commit()
      conn.close()
    })

    //车辆出场数据处理统计、统计结果入库
    val leaveDataGroupByParkID: RDD[(String, Iterable[(String, String, Integer, String)])] = hourLeaveTime.groupBy(_._1)
    val resultForLeaveData: RDD[(String, String, Int, (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int), (Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int), Int)] = leaveDataGroupByParkID.map(line => {
      val list: List[(String, String, Integer, String)] = line._2.toList
      val total: Int = list.length
      var time_1: Int = 0
      var time_2: Int = 0
      var time_3: Int = 0
      var time_4: Int = 0
      var time_5: Int = 0
      var time_6: Int = 0
      var time_7: Int = 0
      var time_8: Int = 0
      var time_9: Int = 0
      var time_10: Int = 0
      var time_11: Int = 0
      var time_12: Int = 0
      var time_13: Int = 0
      var time_14: Int = 0
      var time_15: Int = 0
      var time_16: Int = 0
      var time_17: Int = 0
      var time_18: Int = 0
      var time_19: Int = 0
      var time_20: Int = 0
      var time_21: Int = 0
      var time_22: Int = 0
      var time_23: Int = 0
      var time_24: Int = 0
      for (i <- list) {
        val hour = i._4
        if (hour == "00")
          time_1 = time_1 + 1
        else if (hour == "01")
          time_2 = time_2 + 1
        else if (hour == "02")
          time_3 = time_3 + 1
        else if (hour == "03")
          time_4 = time_4 + 1
        else if (hour == "04")
          time_5 = time_5 + 1
        else if (hour == "05")
          time_6 = time_6 + 1
        else if (hour == "06")
          time_7 = time_7 + 1
        else if (hour == "07")
          time_8 = time_8 + 1
        else if (hour == "08")
          time_9 = time_9 + 1
        else if (hour == "09")
          time_10 = time_10 + 1
        else if (hour == "10")
          time_11 = time_11 + 1
        else if (hour == "11")
          time_12 = time_12 + 1
        else if (hour == "12")
          time_13 = time_13 + 1
        else if (hour == "13")
          time_14 = time_14 + 1
        else if (hour == "14")
          time_15 = time_15 + 1
        else if (hour == "15")
          time_16 = time_16 + 1
        else if (hour == "16")
          time_17 = time_17 + 1
        else if (hour == "17")
          time_18 = time_18 + 1
        else if (hour == "18")
          time_19 = time_19 + 1
        else if (hour == "19")
          time_20 = time_20 + 1
        else if (hour == "20")
          time_21 = time_21 + 1
        else if (hour == "21")
          time_22 = time_22 + 1
        else if (hour == "22")
          time_23 = time_23 + 1
        else if (hour == "23")
          time_24 = time_24 + 1
      }

      //元组中元素个数不能超过22个，将24个time装入两个元组中
      val tuple1 = Tuple12(time_1, time_2, time_3, time_4, time_5, time_6, time_7, time_8, time_9, time_10, time_11, time_12)
      val tuple2 = Tuple12(time_13, time_14, time_15, time_16, time_17, time_18, time_19, time_20, time_21, time_22, time_23, time_24)

      (line._1, dateTime, 1, tuple1, tuple2, total)
    })

    resultForLeaveData.foreachPartition(iter => {
      val conn = DriverManager.getConnection("jdbc:mysql://192.168.1.220:3307/parking?useUnicode=true&characterEncoding=utf8", "parking", "parking@123")
      conn.setAutoCommit(false)
      val preparedStatement = conn.prepareStatement("insert into parking.parkinglot_offline_statistics (park_id,date,access_type,time_1,time_2,time_3,time_4,time_5,time_6,time_7,time_8,time_9,time_10,time_11,time_12,time_13,time_14,time_15,time_16,time_17,time_18,time_19,time_20,time_21,time_22,time_23,time_24,total) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);")
      iter.foreach(t => {
        preparedStatement.setString(1, t._1)
        preparedStatement.setString(2, t._2)
        preparedStatement.setInt(3, t._3)

        preparedStatement.setInt(4, t._4._1)
        preparedStatement.setInt(5, t._4._2)
        preparedStatement.setInt(6, t._4._3)
        preparedStatement.setInt(7, t._4._4)
        preparedStatement.setInt(8, t._4._5)
        preparedStatement.setInt(9, t._4._6)
        preparedStatement.setInt(10, t._4._7)
        preparedStatement.setInt(11, t._4._8)
        preparedStatement.setInt(12, t._4._9)
        preparedStatement.setInt(13, t._4._10)
        preparedStatement.setInt(14, t._4._11)
        preparedStatement.setInt(15, t._4._12)

        preparedStatement.setInt(16, t._5._1)
        preparedStatement.setInt(17, t._5._2)
        preparedStatement.setInt(18, t._5._3)
        preparedStatement.setInt(19, t._5._4)
        preparedStatement.setInt(20, t._5._5)
        preparedStatement.setInt(21, t._5._6)
        preparedStatement.setInt(22, t._5._7)
        preparedStatement.setInt(23, t._5._8)
        preparedStatement.setInt(24, t._5._9)
        preparedStatement.setInt(25, t._5._10)
        preparedStatement.setInt(26, t._5._11)
        preparedStatement.setInt(27, t._5._12)

        preparedStatement.setInt(28, t._6)

        preparedStatement.addBatch()
      })
      preparedStatement.executeBatch()
      conn.commit()
      conn.close()
    })


  }

}
