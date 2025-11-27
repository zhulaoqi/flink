package com.kinch.flink.table;

import com.kinch.flink.common.entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Flink Table API和SQL演示
 * <p>
 * 理论知识：
 * 1. Table API概念：
 * - 关系型API，类似SQL但是用Java/Scala编写
 * - 提供了select、filter、join、groupBy等操作
 * - 可以与DataStream API互转
 * <p>
 * 2. SQL支持：
 * - 完全兼容ANSI SQL
 * - 支持批处理和流处理
 * - 支持窗口聚合、TopN、去重等高级功能
 * <p>
 * 3. 动态表（Dynamic Table）：
 * - 流数据被视为不断变化的表
 * - 查询结果也是动态表
 * - 支持Append、Retract、Upsert三种更新模式
 * <p>
 * 4. 时间属性：
 * - 事件时间（Event Time）：基于数据的时间戳
 * - 处理时间（Processing Time）：基于系统时间
 * - 时间属性是窗口操作的基础
 * <p>
 * 5. 窗口：
 * - 滚动窗口（TUMBLE）：固定大小，不重叠
 * - 滑动窗口（HOP）：固定大小，可重叠
 * - 会话窗口（SESSION）：基于活动间隔
 * - 累积窗口（CUMULATE）：累积聚合
 * <p>
 * 6. 连接器（Connector）：
 * - Kafka、JDBC、Elasticsearch、HBase等
 * - 支持Changelog模式
 * <p>
 * 7. Catalog：
 * - 元数据管理
 * - 支持Hive Catalog、JDBC Catalog等
 */
public class TableAPIDemo {

    public static void main(String[] args) throws Exception {

        // ============================================================
        // 1. 创建执行环境
        // ============================================================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建Table环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ============================================================
        // 2. 创建数据源（使用DataStream转Table）
        // ============================================================
        DataStream<SensorReading> sensorStream = env.fromElements(
                new SensorReading("sensor_1", 1609459200000L, 35.8),
                new SensorReading("sensor_2", 1609459201000L, 15.4),
                new SensorReading("sensor_1", 1609459202000L, 32.1),
                new SensorReading("sensor_3", 1609459203000L, 18.2),
                new SensorReading("sensor_2", 1609459204000L, 23.5),
                new SensorReading("sensor_1", 1609459205000L, 38.3),
                new SensorReading("sensor_3", 1609459206000L, 25.7)
        );

        // 将DataStream转换为Table
        // 指定处理时间属性
        Table sensorTable = tableEnv.fromDataStream(
                sensorStream,
                $("sensorId"),
                $("timestamp"),
                $("temperature"),
                $("proctime").proctime()  // 定义处理时间属性
        );

        // 注册表（可以在SQL中使用）
        tableEnv.createTemporaryView("sensor_readings", sensorTable);

        // ============================================================
        // 3. Table API基础操作
        // ============================================================
        System.out.println("\n========== Table API基础操作 ==========");

        // 3.1 Select：选择列
        Table selectResult = sensorTable.select($("sensorId"), $("temperature"));
        printTable(tableEnv, selectResult, "Select操作");

        // 3.2 Filter：过滤数据
        Table filterResult = sensorTable
                .filter($("temperature").isGreater(20.0))
                .select($("sensorId"), $("temperature"));
        printTable(tableEnv, filterResult, "Filter操作-温度>20");

        // 3.3 GroupBy：分组聚合
        Table groupByResult = sensorTable
                .groupBy($("sensorId"))
                .select(
                        $("sensorId"),
                        $("temperature").avg().as("avgTemp"),
                        $("temperature").max().as("maxTemp"),
                        $("temperature").min().as("minTemp"),
                        $("temperature").count().as("cnt")
                );
        printTable(tableEnv, groupByResult, "GroupBy操作-各传感器统计");

        // ============================================================
        // 4. SQL查询
        // ============================================================
        System.out.println("\n========== SQL查询 ==========");

        // 4.1 基础查询
        Table sqlResult1 = tableEnv.sqlQuery(
                "SELECT sensorId, temperature FROM sensor_readings WHERE temperature > 20"
        );
        printTable(tableEnv, sqlResult1, "SQL查询-温度>20");

        // 4.2 聚合查询
        Table sqlResult2 = tableEnv.sqlQuery(
                "SELECT " +
                        "  sensorId, " +
                        "  AVG(temperature) as avgTemp, " +
                        "  MAX(temperature) as maxTemp, " +
                        "  MIN(temperature) as minTemp, " +
                        "  COUNT(*) as cnt " +
                        "FROM sensor_readings " +
                        "GROUP BY sensorId"
        );
        printTable(tableEnv, sqlResult2, "SQL聚合查询");

        // 4.3 HAVING子句
        Table sqlResult3 = tableEnv.sqlQuery(
                "SELECT " +
                        "  sensorId, " +
                        "  AVG(temperature) as avgTemp " +
                        "FROM sensor_readings " +
                        "GROUP BY sensorId " +
                        "HAVING AVG(temperature) > 25"
        );
        printTable(tableEnv, sqlResult3, "SQL HAVING子句");

        // 4.4 ORDER BY排序
        Table sqlResult4 = tableEnv.sqlQuery(
                "SELECT sensorId, temperature " +
                        "FROM sensor_readings " +
                        "ORDER BY temperature DESC " +
                        "LIMIT 3"
        );
        printTable(tableEnv, sqlResult4, "SQL排序-Top3高温");

        // ============================================================
        // 5. 窗口操作（使用处理时间）
        // ============================================================
        System.out.println("\n========== 窗口操作 ==========");

        // 创建带有处理时间的表
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW sensor_with_proctime AS " +
                        "SELECT *, PROCTIME() as proctime FROM sensor_readings"
        );

        // 5.1 滚动窗口（TUMBLE）- 每5秒统计一次
        Table tumbleWindowResult = tableEnv.sqlQuery(
                "SELECT " +
                        "  sensorId, " +
                        "  TUMBLE_START(proctime, INTERVAL '5' SECOND) as window_start, " +
                        "  TUMBLE_END(proctime, INTERVAL '5' SECOND) as window_end, " +
                        "  AVG(temperature) as avgTemp, " +
                        "  COUNT(*) as cnt " +
                        "FROM sensor_with_proctime " +
                        "GROUP BY sensorId, TUMBLE(proctime, INTERVAL '5' SECOND)"
        );
        System.out.println("滚动窗口查询创建成功");

        // 5.2 滑动窗口（HOP）- 窗口10秒，滑动5秒
        Table hopWindowResult = tableEnv.sqlQuery(
                "SELECT " +
                        "  sensorId, " +
                        "  HOP_START(proctime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_start, " +
                        "  HOP_END(proctime, INTERVAL '5' SECOND, INTERVAL '10' SECOND) as window_end, " +
                        "  AVG(temperature) as avgTemp " +
                        "FROM sensor_with_proctime " +
                        "GROUP BY sensorId, HOP(proctime, INTERVAL '5' SECOND, INTERVAL '10' SECOND)"
        );
        System.out.println("滑动窗口查询创建成功");

        // ============================================================
        // 6. TopN查询
        // ============================================================
        System.out.println("\n========== TopN查询 ==========");

        // 查询每个传感器的最高温度记录
        Table topNResult = tableEnv.sqlQuery(
                "SELECT * FROM (" +
                        "  SELECT *, " +
                        "    ROW_NUMBER() OVER (PARTITION BY sensorId ORDER BY temperature DESC) as rn " +
                        "  FROM sensor_readings" +
                        ") WHERE rn <= 2"
        );
        printTable(tableEnv, topNResult, "TopN-每个传感器Top2高温");

        // ============================================================
        // 7. Join操作
        // ============================================================
        System.out.println("\n========== Join操作 ==========");

        // 创建传感器元数据表
        DataStream<Row> sensorMetaStream = env.fromElements(
                Row.of("sensor_1", "一号车间", "A区"),
                Row.of("sensor_2", "二号车间", "B区"),
                Row.of("sensor_3", "三号车间", "C区")
        );

        Table sensorMetaTable = tableEnv.fromDataStream(
                sensorMetaStream,
                $("sensorId"),
                $("workshop"),
                $("area")
        );

        tableEnv.createTemporaryView("sensor_meta", sensorMetaTable);

        // Join查询：关联传感器数据和元数据
        Table joinResult = tableEnv.sqlQuery(
                "SELECT " +
                        "  r.sensorId, " +
                        "  m.workshop, " +
                        "  m.area, " +
                        "  r.temperature " +
                        "FROM sensor_readings r " +
                        "JOIN sensor_meta m ON r.sensorId = m.sensorId"
        );
        printTable(tableEnv, joinResult, "Join操作-关联元数据");

        // ============================================================
        // 8. 聚合函数
        // ============================================================
        System.out.println("\n========== 聚合函数 ==========");

        Table aggResult = tableEnv.sqlQuery(
                "SELECT " +
                        "  COUNT(*) as total_count, " +
                        "  COUNT(DISTINCT sensorId) as sensor_count, " +
                        "  AVG(temperature) as avg_temp, " +
                        "  MAX(temperature) as max_temp, " +
                        "  MIN(temperature) as min_temp, " +
                        "  SUM(temperature) as sum_temp, " +
                        "  STDDEV_POP(temperature) as stddev_temp " +
                        "FROM sensor_readings"
        );
        printTable(tableEnv, aggResult, "聚合函数");

        // ============================================================
        // 9. CASE WHEN条件表达式
        // ============================================================
        System.out.println("\n========== CASE WHEN表达式 ==========");

        Table caseWhenResult = tableEnv.sqlQuery(
                "SELECT " +
                        "  sensorId, " +
                        "  temperature, " +
                        "  CASE " +
                        "    WHEN temperature < 20 THEN '低温' " +
                        "    WHEN temperature >= 20 AND temperature < 30 THEN '正常' " +
                        "    ELSE '高温' " +
                        "  END as temp_level " +
                        "FROM sensor_readings"
        );
        printTable(tableEnv, caseWhenResult, "CASE WHEN-温度分级");

        // ============================================================
        // 10. 子查询
        // ============================================================
        System.out.println("\n========== 子查询 ==========");

        Table subqueryResult = tableEnv.sqlQuery(
                "SELECT * FROM sensor_readings " +
                        "WHERE temperature > (" +
                        "  SELECT AVG(temperature) FROM sensor_readings" +
                        ")"
        );
        printTable(tableEnv, subqueryResult, "子查询-高于平均温度");

        // ============================================================
        // 11. Table转DataStream
        // ============================================================
        System.out.println("\n========== Table转DataStream ==========");

        // 将Table转换回DataStream
        DataStream<Row> resultStream = tableEnv.toDataStream(filterResult);
        resultStream.print("Table转DataStream");

        // ============================================================
        // 12. DDL创建表
        // ============================================================
        System.out.println("\n========== DDL创建表 ==========");

        // 使用DDL创建表（Datagen连接器生成测试数据）
        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE sensor_source (" +
                        "  sensorId STRING, " +
                        "  temperature DOUBLE, " +
                        "  ts BIGINT, " +
                        "  proctime AS PROCTIME() " +
                        ") WITH (" +
                        "  'connector' = 'datagen', " +
                        "  'rows-per-second' = '1', " +
                        "  'fields.sensorId.length' = '10', " +
                        "  'fields.temperature.min' = '15.0', " +
                        "  'fields.temperature.max' = '40.0' " +
                        ")"
        );

        System.out.println("Datagen源表创建成功");

        // 查询Datagen生成的数据
        Table datagenResult = tableEnv.sqlQuery(
                "SELECT sensorId, temperature FROM sensor_source LIMIT 5"
        );
        System.out.println("Datagen数据查询创建成功");

        // ============================================================
        // 13. Kafka Connector 使用（重要！）
        // ============================================================
        System.out.println("\n========== Kafka Connector SQL ==========");

        /*
        // 创建 Kafka Source 表
        tableEnv.executeSql(
            "CREATE TABLE kafka_sensor_source (" +
            "  sensorId STRING, " +
            "  temperature DOUBLE, " +
            "  ts BIGINT, " +
            "  event_time AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
            "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND " +
            ") WITH (" +
            "  'connector' = 'kafka', " +
            "  'topic' = 'sensor-topic', " +
            "  'properties.bootstrap.servers' = 'localhost:9092', " +
            "  'properties.group.id' = 'flink-sql-group', " +
            "  'scan.startup.mode' = 'latest-offset', " +
            "  'format' = 'json', " +
            "  'json.fail-on-missing-field' = 'false', " +
            "  'json.ignore-parse-errors' = 'true' " +
            ")"
        );

        // 创建 Kafka Sink 表
        tableEnv.executeSql(
            "CREATE TABLE kafka_sensor_sink (" +
            "  sensorId STRING, " +
            "  avgTemp DOUBLE, " +
            "  maxTemp DOUBLE, " +
            "  cnt BIGINT, " +
            "  PRIMARY KEY (sensorId) NOT ENFORCED " +
            ") WITH (" +
            "  'connector' = 'kafka', " +
            "  'topic' = 'sensor-result', " +
            "  'properties.bootstrap.servers' = 'localhost:9092', " +
            "  'format' = 'json', " +
            "  'json.ignore-parse-errors' = 'true' " +
            ")"
        );

        // 流式计算并写入 Kafka
        tableEnv.executeSql(
            "INSERT INTO kafka_sensor_sink " +
            "SELECT " +
            "  sensorId, " +
            "  AVG(temperature) as avgTemp, " +
            "  MAX(temperature) as maxTemp, " +
            "  COUNT(*) as cnt " +
            "FROM kafka_sensor_source " +
            "GROUP BY sensorId"
        );

        System.out.println("Kafka Source/Sink 配置完成");
        System.out.println("说明：需要 Kafka 环境，学习时可以先看代码理解概念");
        */

        // ============================================================
        // 14. Deduplication 去重（重要！）
        // ============================================================
        System.out.println("\n========== Deduplication 去重 ==========");

        // 场景：保留每个传感器的最新数据（基于事件时间）
        // 方式1：使用 ROW_NUMBER（适合事件时间去重）
        Table deduplicatedResult = tableEnv.sqlQuery(
                "SELECT sensorId, timestamp, temperature " +
                        "FROM (" +
                        "  SELECT *, " +
                        "    ROW_NUMBER() OVER (" +
                        "      PARTITION BY sensorId " +
                        "      ORDER BY timestamp DESC" +
                        "    ) as row_num " +
                        "  FROM sensor_readings" +
                        ") WHERE row_num = 1"
        );
        printTable(tableEnv, deduplicatedResult, "去重-保留最新记录");

        // 方式2：使用 LAST_VALUE（更简洁）
        Table lastValueResult = tableEnv.sqlQuery(
                "SELECT " +
                        "  sensorId, " +
                        "  LAST_VALUE(temperature) as latest_temp, " +
                        "  LAST_VALUE(timestamp) as latest_time " +
                        "FROM sensor_readings " +
                        "GROUP BY sensorId"
        );
        printTable(tableEnv, lastValueResult, "去重-LAST_VALUE方式");

        // ============================================================
        // 15. 实时 UV/PV 计算（经典场景）
        // ============================================================
        System.out.println("\n========== UV/PV 计算 ==========");

        // 模拟用户行为数据
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW user_behavior AS " +
                        "SELECT " +
                        "  CAST(sensorId AS STRING) as page_id, " +
                        "  CAST(MOD(CAST(SUBSTR(sensorId, 8) AS INT), 100) AS BIGINT) as user_id, " +
                        "  timestamp as access_time " +
                        "FROM sensor_readings"
        );

        // 计算每个页面的 UV 和 PV
        Table uvPvResult = tableEnv.sqlQuery(
                "SELECT " +
                        "  page_id, " +
                        "  COUNT(*) as pv, " +  // 页面访问量（Page View）
                        "  COUNT(DISTINCT user_id) as uv " +  // 独立访客数（Unique Visitor）
                        "FROM user_behavior " +
                        "GROUP BY page_id"
        );
        printTable(tableEnv, uvPvResult, "UV/PV统计");

        // ============================================================
        // 16. 实时 TopN 排行榜（重要！）
        // ============================================================
        System.out.println("\n========== 实时 TopN 排行榜 ==========");

        // 场景：实时计算温度 Top3 的传感器
        Table topNRankResult = tableEnv.sqlQuery(
                "SELECT sensorId, temperature, rn " +
                        "FROM (" +
                        "  SELECT *, " +
                        "    ROW_NUMBER() OVER (ORDER BY temperature DESC) as rn " +
                        "  FROM sensor_readings" +
                        ") WHERE rn <= 3"
        );
        printTable(tableEnv, topNRankResult, "TopN排行榜-温度Top3");

        // 分组 TopN：每个区域的 Top2
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW sensor_with_area AS " +
                        "SELECT " +
                        "  sensorId, " +
                        "  temperature, " +
                        "  CASE " +
                        "    WHEN sensorId IN ('sensor_1', 'sensor_2') THEN '东区' " +
                        "    WHEN sensorId IN ('sensor_3', 'sensor_4') THEN '西区' " +
                        "    ELSE '南区' " +
                        "  END as area " +
                        "FROM sensor_readings"
        );

        Table groupTopNResult = tableEnv.sqlQuery(
                "SELECT area, sensorId, temperature " +
                        "FROM (" +
                        "  SELECT *, " +
                        "    ROW_NUMBER() OVER (PARTITION BY area ORDER BY temperature DESC) as rn " +
                        "  FROM sensor_with_area" +
                        ") WHERE rn <= 2"
        );
        printTable(tableEnv, groupTopNResult, "分组TopN-每个区域Top2");

        // ============================================================
        // 17. MATCH_RECOGNIZE 模式识别（SQL版本的CEP）
        // ============================================================
        System.out.println("\n========== MATCH_RECOGNIZE 模式识别 ==========");

        // 检测温度连续上升的模式
        Table patternResult = tableEnv.sqlQuery(
                "SELECT * " +
                        "FROM sensor_readings " +
                        "MATCH_RECOGNIZE (" +
                        "  PARTITION BY sensorId " +
                        "  ORDER BY timestamp " +
                        "  MEASURES " +
                        "    FIRST(A.temperature) as start_temp, " +
                        "    LAST(C.temperature) as end_temp, " +
                        "    COUNT(*) as rise_count " +
                        "  PATTERN (A B C) " +
                        "  DEFINE " +
                        "    A AS A.temperature > 20, " +
                        "    B AS B.temperature > A.temperature, " +
                        "    C AS C.temperature > B.temperature " +
                        ") AS T"
        );
        printTable(tableEnv, patternResult, "模式识别-连续上升");

        // ============================================================
        // 18. 时间窗口聚合（实战）
        // ============================================================
        System.out.println("\n========== 时间窗口聚合实战 ==========");

        // 每分钟统计各传感器的平均温度、最高温度
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW sensor_with_eventtime AS " +
                        "SELECT " +
                        "  sensorId, " +
                        "  temperature, " +
                        "  TO_TIMESTAMP(FROM_UNIXTIME(timestamp/1000)) as event_time " +
                        "FROM sensor_readings"
        );

        // 使用滚动窗口
        Table windowAggResult = tableEnv.sqlQuery(
                "SELECT " +
                        "  sensorId, " +
                        "  window_start, " +
                        "  window_end, " +
                        "  AVG(temperature) as avg_temp, " +
                        "  MAX(temperature) as max_temp, " +
                        "  MIN(temperature) as min_temp, " +
                        "  COUNT(*) as data_count " +
                        "FROM TABLE(" +
                        "  TUMBLE(TABLE sensor_with_eventtime, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)" +
                        ") " +
                        "GROUP BY sensorId, window_start, window_end"
        );
        printTable(tableEnv, windowAggResult, "窗口聚合-每分钟统计");

        // ============================================================
        // 19. 数据清洗和转换（ETL）
        // ============================================================
        System.out.println("\n========== 数据清洗和转换 ==========");

        // 数据清洗：过滤异常数据、数据标准化
        Table cleanedData = tableEnv.sqlQuery(
                "SELECT " +
                        "  sensorId, " +
                        "  timestamp, " +
                        "  CASE " +
                        "    WHEN temperature < -50 OR temperature > 100 THEN NULL " +  // 异常值处理
                        "    ELSE temperature " +
                        "  END as temperature, " +
                        "  CASE " +
                        "    WHEN temperature < 0 THEN '异常' " +
                        "    WHEN temperature < 20 THEN '低温' " +
                        "    WHEN temperature < 30 THEN '正常' " +
                        "    ELSE '高温' " +
                        "  END as temp_category " +
                        "FROM sensor_readings " +
                        "WHERE temperature IS NOT NULL"  // 过滤空值
        );
        printTable(tableEnv, cleanedData, "数据清洗");

        // ============================================================
        // 20. 实时指标计算（Dashboard场景）
        // ============================================================
        System.out.println("\n========== 实时指标计算 ==========");

        // 计算多个实时指标
        Table dashboardMetrics = tableEnv.sqlQuery(
                "SELECT " +
                        "  COUNT(*) as total_records, " +  // 总记录数
                        "  COUNT(DISTINCT sensorId) as active_sensors, " +  // 活跃传感器数
                        "  AVG(temperature) as avg_temp_all, " +  // 全局平均温度
                        "  MAX(temperature) as max_temp_all, " +  // 全局最高温度
                        "  MIN(temperature) as min_temp_all, " +  // 全局最低温度
                        "  SUM(CASE WHEN temperature > 30 THEN 1 ELSE 0 END) as high_temp_count, " +  // 高温次数
                        "  SUM(CASE WHEN temperature < 20 THEN 1 ELSE 0 END) as low_temp_count, " +  // 低温次数
                        "  CAST(SUM(CASE WHEN temperature > 30 THEN 1 ELSE 0 END) AS DOUBLE) / " +
                        "    COUNT(*) * 100 as high_temp_ratio " +  // 高温占比（%）
                        "FROM sensor_readings"
        );
        printTable(tableEnv, dashboardMetrics, "实时Dashboard指标");

        // ============================================================
        // 21. 实时同比环比分析
        // ============================================================
        System.out.println("\n========== 同比环比分析 ==========");

        // 使用 LAG 函数计算温度变化率
        Table trendAnalysis = tableEnv.sqlQuery(
                "SELECT " +
                        "  sensorId, " +
                        "  temperature as current_temp, " +
                        "  LAG(temperature, 1) OVER (PARTITION BY sensorId ORDER BY timestamp) as prev_temp, " +
                        "  temperature - LAG(temperature, 1) OVER (PARTITION BY sensorId ORDER BY timestamp) as temp_change, " +
                        "  CASE " +
                        "    WHEN LAG(temperature, 1) OVER (PARTITION BY sensorId ORDER BY timestamp) IS NULL THEN 0 " +
                        "    ELSE (temperature - LAG(temperature, 1) OVER (PARTITION BY sensorId ORDER BY timestamp)) / " +
                        "         LAG(temperature, 1) OVER (PARTITION BY sensorId ORDER BY timestamp) * 100 " +
                        "  END as change_rate " +
                        "FROM sensor_readings"
        );
        printTable(tableEnv, trendAnalysis, "趋势分析-温度变化率");

        // ============================================================
        // 22. 累积窗口（CUMULATE）- Flink 1.13+ 新特性
        // ============================================================
        System.out.println("\n========== 累积窗口 ==========");

        /*
        // 累积窗口：从窗口开始累积到窗口结束
        // 适用场景：实时累积统计，如当天累积GMV
        Table cumulateResult = tableEnv.sqlQuery(
            "SELECT " +
            "  sensorId, " +
            "  window_start, " +
            "  window_end, " +
            "  SUM(temperature) as cumulative_temp, " +
            "  COUNT(*) as cumulative_count " +
            "FROM TABLE(" +
            "  CUMULATE(" +
            "    TABLE sensor_with_eventtime, " +
            "    DESCRIPTOR(event_time), " +
            "    INTERVAL '1' MINUTE, " +  // 步长
            "    INTERVAL '5' MINUTE" +     // 最大窗口
            "  )" +
            ") " +
            "GROUP BY sensorId, window_start, window_end"
        );
        */
        System.out.println("累积窗口说明：适用于实时累积统计场景");

        // ============================================================
        // 23. JDBC Connector 维度表关联（Lookup Join）
        // ============================================================
        System.out.println("\n========== Lookup Join 维度表关联 ==========");

        /*
        // 创建 MySQL 维度表
        tableEnv.executeSql(
            "CREATE TABLE dim_sensor_info (" +
            "  sensorId STRING, " +
            "  sensor_name STRING, " +
            "  location STRING, " +
            "  install_date STRING, " +
            "  PRIMARY KEY (sensorId) NOT ENFORCED " +
            ") WITH (" +
            "  'connector' = 'jdbc', " +
            "  'url' = 'jdbc:mysql://localhost:3306/flink_db', " +
            "  'table-name' = 'sensor_info', " +
            "  'username' = 'root', " +
            "  'password' = 'password', " +
            "  'lookup.cache.max-rows' = '5000', " +  // 缓存配置
            "  'lookup.cache.ttl' = '10min' " +        // 缓存过期时间
            ")"
        );

        // Lookup Join：实时流关联维度表
        Table enrichedResult = tableEnv.sqlQuery(
            "SELECT " +
            "  s.sensorId, " +
            "  s.temperature, " +
            "  d.sensor_name, " +
            "  d.location, " +
            "  d.install_date " +
            "FROM sensor_with_proctime AS s " +
            "LEFT JOIN dim_sensor_info FOR SYSTEM_TIME AS OF s.proctime AS d " +
            "ON s.sensorId = d.sensorId"
        );

        System.out.println("Lookup Join 配置完成");
        System.out.println("说明：需要 MySQL 环境，用于实时关联维度数据");
        */

        // ============================================================
        // 24. 实时监控告警（实战场景）
        // ============================================================
        System.out.println("\n========== 实时监控告警 ==========");

        // 检测温度异常
        Table alertResult = tableEnv.sqlQuery(
                "SELECT " +
                        "  sensorId, " +
                        "  temperature, " +
                        "  CASE " +
                        "    WHEN temperature > 35 THEN '严重告警-温度过高' " +
                        "    WHEN temperature < 15 THEN '严重告警-温度过低' " +
                        "    WHEN temperature > 30 THEN '警告-温度偏高' " +
                        "    WHEN temperature < 18 THEN '警告-温度偏低' " +
                        "    ELSE '正常' " +
                        "  END as alert_level, " +
                        "  timestamp " +
                        "FROM sensor_readings " +
                        "WHERE temperature > 30 OR temperature < 18 " +  // 只输出异常数据
                        "ORDER BY temperature DESC"
        );
        printTable(tableEnv, alertResult, "实时告警");

        // ============================================================
        // 25. 时间窗口 TopN（高级）
        // ============================================================
        System.out.println("\n========== 时间窗口 TopN ==========");

        // 每分钟计算温度 Top3
        Table windowTopNResult = tableEnv.sqlQuery(
                "SELECT window_time, sensorId, avg_temp, rn " +
                        "FROM (" +
                        "  SELECT *, " +
                        "    ROW_NUMBER() OVER (PARTITION BY window_time ORDER BY avg_temp DESC) as rn " +
                        "  FROM (" +
                        "    SELECT " +
                        "      TUMBLE_END(proctime, INTERVAL '1' MINUTE) as window_time, " +
                        "      sensorId, " +
                        "      AVG(temperature) as avg_temp " +
                        "    FROM sensor_with_proctime " +
                        "    GROUP BY sensorId, TUMBLE(proctime, INTERVAL '1' MINUTE)" +
                        "  )" +
                        ") WHERE rn <= 3"
        );
        System.out.println("窗口TopN查询创建成功");

        // ============================================================
        // 26. JDBC Sink 写入数据库
        // ============================================================
        System.out.println("\n========== JDBC Sink 写入数据库 ==========");

        /*
        // 创建 MySQL Sink 表
        tableEnv.executeSql(
            "CREATE TABLE mysql_sensor_stats (" +
            "  sensorId STRING, " +
            "  avg_temperature DOUBLE, " +
            "  max_temperature DOUBLE, " +
            "  min_temperature DOUBLE, " +
            "  data_count BIGINT, " +
            "  update_time TIMESTAMP(3), " +
            "  PRIMARY KEY (sensorId) NOT ENFORCED " +
            ") WITH (" +
            "  'connector' = 'jdbc', " +
            "  'url' = 'jdbc:mysql://localhost:3306/flink_db', " +
            "  'table-name' = 'sensor_stats', " +
            "  'username' = 'root', " +
            "  'password' = 'password', " +
            "  'sink.buffer-flush.max-rows' = '100', " +  // 批量大小
            "  'sink.buffer-flush.interval' = '1s' " +    // 刷新间隔
            ")"
        );

        // 实时统计写入数据库
        tableEnv.executeSql(
            "INSERT INTO mysql_sensor_stats " +
            "SELECT " +
            "  sensorId, " +
            "  AVG(temperature) as avg_temperature, " +
            "  MAX(temperature) as max_temperature, " +
            "  MIN(temperature) as min_temperature, " +
            "  COUNT(*) as data_count, " +
            "  CURRENT_TIMESTAMP as update_time " +
            "FROM sensor_readings " +
            "GROUP BY sensorId"
        );

        System.out.println("JDBC Sink 配置完成");
        */

        // ============================================================
        // 27. Print Connector（调试利器）
        // ============================================================
        System.out.println("\n========== Print Connector ==========");

        // 创建 Print Sink 用于调试
        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE print_sink (" +
                        "  sensorId STRING, " +
                        "  temperature DOUBLE " +
                        ") WITH (" +
                        "  'connector' = 'print' " +
                        ")"
        );

        // 查询结果输出到 Print Sink
        tableEnv.executeSql(
                "INSERT INTO print_sink " +
                        "SELECT sensorId, temperature " +
                        "FROM sensor_readings " +
                        "WHERE temperature > 25"
        );

        System.out.println("Print Sink 已配置（会输出到控制台）");

        env.execute("Flink Table API Demo");

        env.execute("Flink Table API Demo");

        /*
         * Table API和SQL总结：
         *
         * 1. 使用场景：
         *    - 关系型数据处理：过滤、聚合、Join
         *    - ETL任务：数据清洗、转换
         *    - 实时数仓：维度表Join、指标计算
         *    - 复杂分析：窗口聚合、TopN、去重
         *
         * 2. Table API vs SQL：
         *    - Table API：类型安全、IDE提示、易于重构
         *    - SQL：通用、易学、支持动态查询
         *    - 可以混合使用
         *
         * 3. 性能优化：
         *    - 使用Group Aggregate代替Window Aggregate（无窗口需求时）
         *    - 合理设置State TTL
         *    - 使用MiniBatch聚合减少状态访问
         *    - 使用Local-Global聚合减少数据shuffle
         *
         * 4. 状态管理：
         *    - Group Aggregate：每个key一个状态
         *    - Window Aggregate：每个窗口一个状态，窗口关闭后清理
         *    - Join：保留所有历史数据（需要State TTL）
         *
         * 5. 时间语义：
         *    - 处理时间：简单、低延迟、不确定
         *    - 事件时间：准确、可重放、需要Watermark
         *
         * 6. 输出模式：
         *    - Append：只追加（适合窗口聚合）
         *    - Retract：可撤回（适合普通聚合）
         *    - Upsert：更新插入（需要主键）
         *
         * 7. 连接器使用：
         *    - Source：Kafka、JDBC、File、Datagen
         *    - Sink：Kafka、JDBC、Elasticsearch、File
         *    - Lookup：维度表Join（如MySQL、Redis）
         *
         * 8. 生产环境建议：
         *    - 设置State TTL防止状态无限增长
         *    - 监控状态大小和Checkpoint时长
         *    - 使用Catalog管理元数据
         *    - 合理使用缓存和预聚合
         */
    }

    /**
     * 打印表内容（用于演示）
     */
    private static void printTable(StreamTableEnvironment tableEnv, Table table, String title) {
        System.out.println("\n--- " + title + " ---");
        try {
            // 收集结果（仅用于演示，生产环境不要这样做）
            tableEnv.toDataStream(table).print();
        } catch (Exception e) {
            System.out.println("注意：此查询需要在流式执行环境中运行");
        }
    }
}

