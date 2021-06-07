package org.apache.spark.sql

import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

/**
 * @author Geek
 * @date 2021-06-07 15:03:53
 * 
 * from_json 测试类运行准备：
 * 1. git checkout v3.1.2
 * 2. 在项目路径下打开 git cmd，执行命令： ./build/spark-build-info ./core/target/extra-resources 3.1.2
 *    https://stackoverflow.com/a/44416809/9633499
 * 3. maven --> spark-catalyst --> 右键 generate sources and update folders
 *    https://stackoverflow.com/a/63202594/9633499
 *    
 * 参考文档： spark 发行说明：https://spark.apache.org/news/index.html
 * https://spark.apache.org/releases/spark-release-3-0-0.html
 */
class FromJsonTest extends QueryTest with SharedSparkSession{
  
  import testImplicits._
  
  /**
   * 运行后异常提示转换出错
   */
  test("from_json_1_with_FAILFAST") {
    val df1 = Seq(
      """{"a_double":0.01,"b_int":123,"c_bool":false,"d_string":"hello","e_long":123456789012}""",
      """{"a_double":"0.31","b_int":123,"c_bool":false,"d_string":"hello","e_long":123456789012}""",
      """{"a_double":0.012,"b_int":456,"c_bool":true,"d_string":"world","e_long":123456789022}"""
    ).toDF("value")
    
    df1.printSchema()
    df1.show(false)
    
    println("----------")
    
    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse("""{"type":"record","name":"schema","fields":[{"name":"a_double","type":["double"]},{"name":"b_int","type":["int"]},{"name":"c_bool","type":["boolean"]},{"name":"d_string","type":["null","string"],"default":null},{"name":"e_long","type":["null","long"],"default":null}]}""")
    ).dataType
    
    val options = Map[String, String]("mode" -> "FAILFAST")
    val df2 = df1.select(
      from_json(col("value"), dataType, options).as("json")
    ).select("json.*")
    
    df2.printSchema()
    /*
----------
root
 |-- a_double: double (nullable = true)
 |-- b_int: integer (nullable = true)
 |-- c_bool: boolean (nullable = true)
 |-- d_string: string (nullable = true)
 |-- e_long: long (nullable = true)    
     */
    df2.show(false)
    /*
org.apache.spark.SparkException: Malformed records are detected in record parsing. Parse Mode: FAILFAST. To process malformed records as null result, try setting the option 'mode' as 'PERMISSIVE'.

Caused by: org.apache.spark.sql.catalyst.util.BadRecordException: java.lang.RuntimeException: Cannot parse 0.31 as double.    
     */
  }
  
  /**
   * 正常运行
   */
  test("from_json_1_without_FAILFAST") {
    val df1 = Seq(
      """{"a_double":0.01,"b_int":123,"c_bool":false,"d_string":"hello","e_long":123456789012}""",
      """{"a_double":"0.31","b_int":123,"c_bool":false,"d_string":"hello","e_long":123456789012}""",
      """{"a_double":0.012,"b_int":456,"c_bool":true,"d_string":"world","e_long":123456789022}"""
    ).toDF("value")
    
    df1.printSchema()
    df1.show(false)
    
    println("----------")
    
    val dataType = SchemaConverters.toSqlType(
      new Schema.Parser().parse("""{"type":"record","name":"schema","fields":[{"name":"a_double","type":["double"]},{"name":"b_int","type":["int"]},{"name":"c_bool","type":["boolean"]},{"name":"d_string","type":["null","string"],"default":null},{"name":"e_long","type":["null","long"],"default":null}]}""")
    ).dataType
    
    //val options = Map[String, String]("mode" -> "FAILFAST")
    val df2 = df1.select(
      from_json(col("value"), dataType).as("json")
    )
      .select("json.*")
    
    df2.printSchema()
    df2.show(false)
    /*
+--------+-----+------+--------+------------+
|a_double|b_int|c_bool|d_string|e_long      |
+--------+-----+------+--------+------------+
|0.01    |123  |false |hello   |123456789012|
|null    |123  |false |hello   |123456789012|
|0.012   |456  |true  |world   |123456789022|
+--------+-----+------+--------+------------+    
     */
  }
}
