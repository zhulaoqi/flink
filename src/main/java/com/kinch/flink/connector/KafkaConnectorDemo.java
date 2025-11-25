package com.kinch.flink.connector;

import com.kinch.flink.common.entity.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.regex.Pattern;

/**
 * Flink Kafka连接器演示
 * <p>
 * 理论知识：
 * 1. Kafka连接器作用：
 * - Source：从Kafka读取数据
 * - Sink：向Kafka写入数据
 * - 支持Exactly-Once语义
 * <p>
 * 2. KafkaSource特性：
 * - 动态分区发现：自动发现新增分区
 * - 多Topic订阅：支持正则表达式
 * - Offset管理：支持从指定位置消费
 * - Watermark生成：支持事件时间处理
 * <p>
 * 3. KafkaSink特性：
 * - 精确一次语义：基于Kafka事务
 * - 至少一次语义：无事务开销
 * - 分区策略：自定义分区器
 * - 序列化：自定义序列化器
 * <p>
 * 4. 消费语义：
 * - At-Most-Once：最多一次（可能丢失）
 * - At-Least-Once：至少一次（可能重复）
 * - Exactly-Once：精确一次（推荐）
 * <p>
 * 5. Offset提交策略：
 * - 自动提交：Flink Checkpoint完成后提交
 * - 禁用自动提交：由Flink管理Offset
 * <p>
 * 6. 性能优化：
 * - 调整fetch.min.bytes和fetch.max.wait.ms
 * - 设置合理的max.poll.records
 * - 使用批量写入
 */
public class KafkaConnectorDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 启用Checkpoint（Exactly-Once必需）
        env.enableCheckpointing(5000);

        // ============================================================
        // 1. KafkaSource基础配置
        // ============================================================
        System.out.println("\n========== Kafka Source配置 ==========");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                // Kafka broker地址
                .setBootstrapServers("localhost:9092")
                // 订阅Topic
                .setTopics("sensor-topic")
                // 消费者组ID
                .setGroupId("flink-consumer-group")
                // Offset起始位置：earliest（最早）/ latest（最新）/ timestamp（指定时间）
                .setStartingOffsets(OffsetsInitializer.earliest())
                // 反序列化器
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 属性配置
                .setProperty("partition.discovery.interval.ms", "10000")  // 动态分区发现间隔
                .setProperty("enable.auto.commit", "false")  // 禁用自动提交，由Flink管理
                .build();

        // 从Kafka读取数据
        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka-Source"
        );

        kafkaStream.print("Kafka输入");

        // ============================================================
        // 2. KafkaSource高级配置
        // ============================================================
        System.out.println("\n========== Kafka Source高级配置 ==========");

        // 2.1 订阅多个Topic
        KafkaSource<String> multiTopicSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("topic1", "topic2", "topic3")  // 多个Topic
                .setGroupId("multi-topic-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 2.2 使用正则表达式订阅Topic
        KafkaSource<String> patternTopicSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopicPattern(Pattern.compile("sensor-.*"))
                .setGroupId("pattern-topic-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 2.3 从指定时间戳开始消费
        KafkaSource<String> timestampSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("sensor-topic")
                .setGroupId("timestamp-group")
                .setStartingOffsets(OffsetsInitializer.timestamp(1609459200000L))  // 从指定时间戳开始
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 2.4 从指定Offset开始消费
        // OffsetsInitializer.offsets(Map<TopicPartition, Long> offsets)

        // ============================================================
        // 3. KafkaSink基础配置
        // ============================================================
        System.out.println("\n========== Kafka Sink配置 ==========");

        // 处理数据
        DataStream<String> processedStream = kafkaStream
                .map(data -> "Processed: " + data)
                .name("Process-Data");

        // 3.1 Exactly-Once语义的KafkaSink
        KafkaSink<String> exactlyOnceSink = KafkaSink.<String>builder()
                // Kafka broker地址
                .setBootstrapServers("localhost:9092")
                // 序列化器
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("output-topic")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 投递保证：精确一次
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 事务ID前缀（Exactly-Once必需）
                .setTransactionalIdPrefix("flink-kafka-sink")
                // Kafka生产者属性
                .setProperty("transaction.timeout.ms", "900000")  // 事务超时时间
                .build();

        processedStream.sinkTo(exactlyOnceSink).name("Kafka-Sink-Exactly-Once");

        // 3.2 At-Least-Once语义的KafkaSink（性能更好）
        KafkaSink<String> atLeastOnceSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("output-topic-at-least-once")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 投递保证：至少一次
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 3.3 None语义的KafkaSink（不保证）
        KafkaSink<String> noneSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("output-topic-none")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.NONE)
                .build();

        // ============================================================
        // 4. 自定义分区策略
        // ============================================================
        System.out.println("\n========== 自定义分区策略 ==========");

        // 根据传感器ID进行分区
        // 注意：Flink 1.18.1 的 Kafka Connector 不再支持自定义分区器
// 如果需要自定义分区，可以在发送前通过KeyBy进行数据分区
// 或者使用 setKeySerializationSchema 指定key来影响分区

// 根据传感器ID进行分区的替代方案：使用KeyBy
        DataStream<String> partitionedStream = processedStream
                .keyBy(element -> element.hashCode() % 10);  // 手动分区

// 然后使用普通的Kafka Sink
        KafkaSink<String> partitionedSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("partitioned-topic")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                // 不使用 setPartitioner
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // ============================================================
        // 5. 动态Topic路由
        // ============================================================
        System.out.println("\n========== 动态Topic路由 ==========");

        // 根据数据内容动态选择Topic
        KafkaSink<String> dynamicTopicSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()  // 添加泛型
                                // 动态Topic选择
                                .setTopicSelector((String element) -> {  // 明确指定类型为 String
                                    // 根据数据内容选择Topic
                                    if (element.contains("error")) {
                                        return "error-topic";
                                    } else if (element.contains("warning")) {
                                        return "warning-topic";
                                    } else {
                                        return "info-topic";
                                    }
                                })
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // ============================================================
        // 6. Kafka性能优化配置
        // ============================================================
        System.out.println("\n========== 性能优化配置 ==========");

        KafkaSource<String> optimizedSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("sensor-topic")
                .setGroupId("optimized-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 性能优化参数
                .setProperty("fetch.min.bytes", "1024")  // 最小拉取字节数
                .setProperty("fetch.max.wait.ms", "500")  // 最大等待时间
                .setProperty("max.poll.records", "500")  // 单次拉取最大记录数
                .setProperty("receive.buffer.bytes", "65536")  // 接收缓冲区大小
                .setProperty("send.buffer.bytes", "131072")  // 发送缓冲区大小
                .build();

        KafkaSink<String> optimizedSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("output-topic")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                // 生产者性能优化
                .setProperty("batch.size", "16384")  // 批量大小
                .setProperty("linger.ms", "10")  // 延迟发送时间
                .setProperty("compression.type", "snappy")  // 压缩类型：none/gzip/snappy/lz4/zstd
                .setProperty("buffer.memory", "33554432")  // 缓冲区内存
                .build();

        env.execute("Flink Kafka Connector Demo");

        /*
         * Kafka连接器使用总结：
         *
         * 1. Source配置要点：
         *    - BootstrapServers：Kafka集群地址
         *    - Topics：订阅的Topic列表
         *    - GroupId：消费者组ID
         *    - StartingOffsets：起始消费位置
         *    - Deserializer：反序列化器
         *
         * 2. Sink配置要点：
         *    - BootstrapServers：Kafka集群地址
         *    - RecordSerializer：序列化器配置
         *    - DeliveryGuarantee：投递保证级别
         *    - TransactionalIdPrefix：事务ID前缀（Exactly-Once必需）
         *
         * 3. Exactly-Once实现原理：
         *    - 基于Kafka事务和Flink Checkpoint
         *    - Source：将Offset作为State保存
         *    - Sink：使用两阶段提交协议
         *    - Checkpoint完成后提交事务
         *
         * 4. 性能优化建议：
         *    a. Source端：
         *       - 增加fetch.min.bytes减少网络往返
         *       - 调整max.poll.records控制拉取量
         *       - 增加并行度提高吞吐量
         *
         *    b. Sink端：
         *       - 使用At-Least-Once代替Exactly-Once（如果允许）
         *       - 增加batch.size和linger.ms提高批量
         *       - 启用压缩减少网络传输
         *       - 增加buffer.memory提高缓冲能力
         *
         * 5. Offset管理：
         *    - Flink管理Offset，存储在State中
         *    - Checkpoint完成后提交Offset到Kafka
         *    - 故障恢复时从State恢复Offset
         *    - 不建议使用Kafka自动提交
         *
         * 6. 分区发现：
         *    - 设置partition.discovery.interval.ms
         *    - 定期扫描新增分区
         *    - 动态调整并行度
         *
         * 7. 消费滞后监控：
         *    - 监控消费Lag
         *    - 设置告警阈值
         *    - 及时扩容或优化
         *
         * 8. 生产环境建议：
         *    - 优先使用Exactly-Once（金融、支付）
         *    - 一般场景使用At-Least-Once + 幂等
         *    - 启用Checkpoint确保容错
         *    - 合理设置并行度
         *    - 监控消费Lag和吞吐量
         *
         * 9. 常见问题：
         *    a. Rebalance频繁：
         *       - 增加session.timeout.ms
         *       - 减少max.poll.records
         *       - 优化处理逻辑
         *
         *    b. 消费延迟高：
         *       - 增加并行度
         *       - 优化处理逻辑
         *       - 检查网络和Kafka性能
         *
         *    c. 事务超时：
         *       - 增加transaction.timeout.ms
         *       - 减少Checkpoint间隔
         *       - 优化Sink性能
         *
         * 10. 版本兼容性：
         *     - Flink 1.18.x 支持 Kafka 2.4.0+
         *     - 建议使用相同大版本
         *     - 注意API变更
         */
    }
}

