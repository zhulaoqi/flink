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

