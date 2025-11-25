package com.kinch.flink.connector;

import com.kinch.flink.common.entity.SensorReading;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Flink JDBC连接器演示
 * <p>
 * 理论知识：
 * 1. JDBC连接器作用：
 * - Source：从数据库读取数据（批量或流式）
 * - Sink：向数据库写入数据
 * - Lookup：维度表Join
 * <p>
 * 2. JdbcSink特性：
 * - 批量写入：提高吞吐量
 * - 自动重试：网络异常自动重试
 * - Exactly-Once：基于XA事务或幂等写入
 * - 连接池：复用数据库连接
 * <p>
 * 3. 写入模式：
 * - INSERT：插入新记录
 * - UPDATE：更新现有记录
 * - UPSERT：存在则更新，不存在则插入
 * - DELETE：删除记录
 * <p>
 * 4. Exactly-Once实现：
 * - XA事务：支持XA协议的数据库（MySQL、PostgreSQL）
 * - 幂等写入：基于唯一键的UPSERT
 * - At-Least-Once + 去重表
 * <p>
 * 5. 性能优化：
 * - 批量写入：减少数据库交互次数
 * - 连接池：复用连接
 * - 异步写入：提高吞吐量
 * - 索引优化：提高查询和写入性能
 */
public class JDBCConnectorDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 启用Checkpoint
        env.enableCheckpointing(5000);

        // 创建测试数据
        DataStream<SensorReading> sensorStream = env.fromElements(
                new SensorReading("sensor_1", 1609459200000L, 35.8),
                new SensorReading("sensor_2", 1609459201000L, 15.4),
                new SensorReading("sensor_1", 1609459202000L, 32.1),
                new SensorReading("sensor_3", 1609459203000L, 18.2),
                new SensorReading("sensor_2", 1609459204000L, 23.5)
        );

        // ============================================================
        // 1. JdbcSink基础配置（INSERT模式）
        // ============================================================
        System.out.println("\n========== JDBC Sink基础配置 ==========");
        
        /*
        // 创建MySQL表（示例SQL）
        CREATE TABLE sensor_readings (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            sensor_id VARCHAR(50) NOT NULL,
            timestamp BIGINT NOT NULL,
            temperature DOUBLE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_sensor_timestamp (sensor_id, timestamp)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        */

        SinkFunction<SensorReading> jdbcSink = JdbcSink.sink(
                // SQL语句
                "INSERT INTO sensor_readings (sensor_id, timestamp, temperature) VALUES (?, ?, ?)",

                // 参数设置
                (PreparedStatement ps, SensorReading reading) -> {
                    ps.setString(1, reading.getSensorId());
                    ps.setLong(2, reading.getTimestamp());
                    ps.setDouble(3, reading.getTemperature());
                },

                // 执行选项
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)  // 批量大小：100条
                        .withBatchIntervalMs(1000)  // 批量间隔：1秒
                        .withMaxRetries(3)  // 最大重试次数
                        .build(),

                // 连接选项
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/flink_db?useUnicode=true&characterEncoding=UTF-8")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("password")
                        .build()
        );

        sensorStream.addSink(jdbcSink).name("JDBC-Sink-Insert");

        // ============================================================
        // 2. UPSERT模式（存在则更新，不存在则插入）
        // ============================================================
        System.out.println("\n========== UPSERT模式 ==========");
        
        /*
        // 创建带唯一键的表
        CREATE TABLE sensor_latest (
            sensor_id VARCHAR(50) PRIMARY KEY,
            timestamp BIGINT NOT NULL,
            temperature DOUBLE NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        */

        SinkFunction<SensorReading> upsertSink = JdbcSink.sink(
                // MySQL UPSERT语法（ON DUPLICATE KEY UPDATE）
                "INSERT INTO sensor_latest (sensor_id, timestamp, temperature) " +
                        "VALUES (?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE timestamp = VALUES(timestamp), temperature = VALUES(temperature)",

                (PreparedStatement ps, SensorReading reading) -> {
                    ps.setString(1, reading.getSensorId());
                    ps.setLong(2, reading.getTimestamp());
                    ps.setDouble(3, reading.getTemperature());
                },

                JdbcExecutionOptions.builder()
                        .withBatchSize(50)
                        .withBatchIntervalMs(500)
                        .withMaxRetries(5)
                        .build(),

                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/flink_db")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("password")
                        .build()
        );

        sensorStream.addSink(upsertSink).name("JDBC-Sink-Upsert");

        // ============================================================
        // 3. 批量更新模式
        // ============================================================
        System.out.println("\n========== 批量更新模式 ==========");

        SinkFunction<SensorReading> updateSink = JdbcSink.sink(
                "UPDATE sensor_readings SET temperature = ? WHERE sensor_id = ? AND timestamp = ?",

                (PreparedStatement ps, SensorReading reading) -> {
                    ps.setDouble(1, reading.getTemperature());
                    ps.setString(2, reading.getSensorId());
                    ps.setLong(3, reading.getTimestamp());
                },

                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(3)
                        .build(),

                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/flink_db")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("password")
                        .build()
        );

        // ============================================================
        // 4. Exactly-Once语义（基于XA事务）
        // ============================================================
        System.out.println("\n========== Exactly-Once语义 ==========");
        
        /*
        // 注意：XA事务需要：
        // 1. 数据库支持XA协议（MySQL 5.7+, PostgreSQL）
        // 2. 启用Checkpoint
        // 3. 配置合适的超时时间
        
        SinkFunction<SensorReading> exactlyOnceSink = JdbcSink.exactlyOnceSink(
            "INSERT INTO sensor_readings (sensor_id, timestamp, temperature) VALUES (?, ?, ?)",
            
            JdbcStatementBuilder<SensorReading>() {
                @Override
                public void accept(PreparedStatement ps, SensorReading reading) throws SQLException {
                    ps.setString(1, reading.getSensorId());
                    ps.setLong(2, reading.getTimestamp());
                    ps.setDouble(3, reading.getTemperature());
                }
            },
            
            JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withBatchIntervalMs(1000)
                .withMaxRetries(3)
                .build(),
            
            JdbcExactlyOnceOptions.builder()
                .withTransactionPerConnection(true)
                .build(),
            
            () -> {
                // XA数据源配置
                PGXADataSource xaDataSource = new PGXADataSource();
                xaDataSource.setUrl("jdbc:postgresql://localhost:5432/flink_db");
                xaDataSource.setUser("postgres");
                xaDataSource.setPassword("password");
                return xaDataSource;
            }
        );
        */

        // ============================================================
        // 5. 连接池配置优化
        // ============================================================
        System.out.println("\n========== 连接池配置 ==========");

        // 使用HikariCP连接池（推荐）
        SinkFunction<SensorReading> optimizedSink = JdbcSink.sink(
                "INSERT INTO sensor_readings (sensor_id, timestamp, temperature) VALUES (?, ?, ?)",

                (PreparedStatement ps, SensorReading reading) -> {
                    ps.setString(1, reading.getSensorId());
                    ps.setLong(2, reading.getTimestamp());
                    ps.setDouble(3, reading.getTemperature());
                },

                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)  // 增大批量大小
                        .withBatchIntervalMs(2000)  // 增大批量间隔
                        .withMaxRetries(5)
                        .build(),

                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/flink_db?" +
                                "useUnicode=true&" +
                                "characterEncoding=UTF-8&" +
                                "useSSL=false&" +
                                "serverTimezone=Asia/Shanghai&" +
                                "rewriteBatchedStatements=true&" + // 启用批量重写（性能优化）
                                "cachePrepStmts=true&" +  // 缓存PreparedStatement
                                "prepStmtCacheSize=250&" +
                                "prepStmtCacheSqlLimit=2048")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("password")
                        .build()
        );

        // ============================================================
        // 6. 错误处理和监控
        // ============================================================
        System.out.println("\n========== 错误处理 ==========");

        // 添加异常处理
        DataStream<SensorReading> validatedStream = sensorStream
                .map(reading -> {
                    // 数据验证
                    if (reading.getSensorId() == null || reading.getSensorId().isEmpty()) {
                        throw new IllegalArgumentException("传感器ID不能为空");
                    }
                    if (reading.getTemperature() < -50 || reading.getTemperature() > 100) {
                        throw new IllegalArgumentException("温度值异常：" + reading.getTemperature());
                    }
                    return reading;
                })
                .name("Validate-Data");

        validatedStream.addSink(jdbcSink).name("JDBC-Sink-With-Validation");

        env.execute("Flink JDBC Connector Demo");

        /*
         * JDBC连接器使用总结：
         *
         * 1. 基础配置：
         *    - SQL语句：INSERT/UPDATE/UPSERT/DELETE
         *    - 参数绑定：JdbcStatementBuilder
         *    - 批量配置：BatchSize和BatchInterval
         *    - 连接配置：URL、Driver、Username、Password
         *
         * 2. 性能优化：
         *    a. 批量写入：
         *       - 增大BatchSize（1000-5000）
         *       - 合理设置BatchInterval（1-5秒）
         *       - 启用rewriteBatchedStatements
         *
         *    b. 连接池：
         *       - 使用HikariCP或Druid
         *       - 设置合理的连接池大小
         *       - 启用PreparedStatement缓存
         *
         *    c. 数据库优化：
         *       - 创建合适的索引
         *       - 调整innodb_buffer_pool_size
         *       - 使用SSD存储
         *       - 分库分表（大数据量）
         *
         * 3. Exactly-Once实现方案：
         *    a. XA事务（推荐）：
         *       - 支持的数据库：MySQL 5.7+、PostgreSQL
         *       - 需要启用Checkpoint
         *       - 性能开销较大
         *
         *    b. 幂等写入：
         *       - 使用UPSERT语句
         *       - 基于唯一键去重
         *       - 性能好，推荐使用
         *
         *    c. 去重表：
         *       - 写入时记录唯一ID
         *       - 定期清理历史记录
         *       - 适合At-Least-Once场景
         *
         * 4. 错误处理：
         *    - 配置合理的MaxRetries
         *    - 监控写入失败率
         *    - 记录异常数据到日志或侧输出流
         *    - 设置告警阈值
         *
         * 5. 监控指标：
         *    - 写入吞吐量（records/s）
         *    - 写入延迟（P50/P95/P99）
         *    - 失败重试次数
         *    - 数据库连接数
         *    - 批量大小分布
         *
         * 6. 常见问题：
         *    a. 连接池耗尽：
         *       - 增加连接池大小
         *       - 检查连接泄漏
         *       - 优化SQL执行时间
         *
         *    b. 写入慢：
         *       - 增大批量大小
         *       - 优化SQL和索引
         *       - 检查数据库性能
         *
         *    c. 主键冲突：
         *       - 使用UPSERT代替INSERT
         *       - 检查数据源重复
         *       - 添加唯一键约束
         *
         * 7. 生产环境建议：
         *    - 使用连接池（HikariCP）
         *    - 启用批量写入
         *    - 配置合理的超时和重试
         *    - 监控写入性能
         *    - 定期优化数据库
         *    - 考虑读写分离
         *    - 大表考虑分区
         *
         * 8. 不同数据库的UPSERT语法：
         *    - MySQL: INSERT ... ON DUPLICATE KEY UPDATE
         *    - PostgreSQL: INSERT ... ON CONFLICT ... DO UPDATE
         *    - SQL Server: MERGE INTO
         *    - Oracle: MERGE INTO
         *    - SQLite: INSERT OR REPLACE
         *
         * 9. 安全建议：
         *    - 使用参数化查询防止SQL注入
         *    - 加密数据库密码
         *    - 使用最小权限账户
         *    - 启用SSL连接
         *    - 定期更新JDBC驱动
         */
    }
}

