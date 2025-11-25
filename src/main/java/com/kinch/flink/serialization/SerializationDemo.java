package com.kinch.flink.serialization;

import com.alibaba.fastjson.JSON;
import com.kinch.flink.common.entity.SensorReading;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Flink 序列化（Serialization）演示
 * <p>
 * 理论知识：
 * 1. 序列化概念：
 * - 将对象转换为字节流进行传输或存储
 * - 反序列化：将字节流还原为对象
 * - Flink中序列化用于网络传输和State存储
 * <p>
 * 2. Flink序列化框架：
 * - TypeInformation：Flink的类型系统
 * - TypeSerializer：具体的序列化器
 * - 自动推导：大多数情况自动推导类型
 * <p>
 * 3. 序列化器类型：
 * - POJOSerializer：处理POJO类（推荐）
 * - TupleSerializer：处理Tuple类型
 * - CaseClassSerializer：处理Scala Case Class
 * - AvroSerializer：处理Avro对象
 * - KryoSerializer：通用序列化器（性能较差）
 * <p>
 * 4. POJO要求：
 * - 公共类
 * - 无参构造函数
 * - 所有字段public或有getter/setter
 * - 字段类型必须是Flink支持的类型
 * <p>
 * 5. 自定义序列化：
 * - DeserializationSchema：反序列化接口
 * - SerializationSchema：序列化接口
 * - 用于Kafka、文件等连接器
 * <p>
 * 6. 序列化格式：
 * - JSON：可读性好，性能一般
 * - Avro：Schema演化支持，二进制格式
 * - Protobuf：高性能，跨语言
 * - Kryo：Java序列化，通用但慢
 * <p>
 * 7. 性能对比：
 * - POJO > Tuple > Avro > JSON > Kryo
 * - 生产环境推荐：POJO或Avro
 */
public class SerializationDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ============================================================
        // 1. POJO序列化（推荐）
        // ============================================================
        System.out.println("\n========== POJO序列化 ==========");

        // SensorReading是标准的POJO类，Flink会自动使用POJOSerializer
        DataStream<SensorReading> sensorStream = env.fromElements(
                new SensorReading("sensor_1", 1609459200000L, 35.8),
                new SensorReading("sensor_2", 1609459201000L, 15.4),
                new SensorReading("sensor_3", 1609459202000L, 25.6)
        );

        sensorStream.print("POJO序列化");

        // ============================================================
        // 2. JSON序列化/反序列化
        // ============================================================
        System.out.println("\n========== JSON序列化 ==========");

        // 自定义JSON序列化器
        SerializationSchema<SensorReading> jsonSerializer = new SerializationSchema<SensorReading>() {
            @Override
            public byte[] serialize(SensorReading element) {
                return JSON.toJSONString(element).getBytes(StandardCharsets.UTF_8);
            }
        };

        // 自定义JSON反序列化器
        DeserializationSchema<SensorReading> jsonDeserializer = new DeserializationSchema<SensorReading>() {
            @Override
            public SensorReading deserialize(byte[] message) throws IOException {
                String jsonStr = new String(message, StandardCharsets.UTF_8);
                return JSON.parseObject(jsonStr, SensorReading.class);
            }

            @Override
            public boolean isEndOfStream(SensorReading nextElement) {
                return false;
            }

            @Override
            public TypeInformation<SensorReading> getProducedType() {
                return TypeInformation.of(SensorReading.class);
            }
        };

        // 序列化为JSON字符串
        DataStream<byte[]> serializedStream = sensorStream.map(
                sensor -> jsonSerializer.serialize(sensor)
        );

        // 反序列化JSON字符串
        DataStream<SensorReading> deserializedStream = serializedStream.map(
                bytes -> jsonDeserializer.deserialize(bytes)
        );

        deserializedStream.print("JSON反序列化");

        // ============================================================
        // 3. 自定义格式序列化（CSV）
        // ============================================================
        System.out.println("\n========== CSV序列化 ==========");

        // CSV序列化器
        SerializationSchema<SensorReading> csvSerializer = new SerializationSchema<SensorReading>() {
            @Override
            public byte[] serialize(SensorReading element) {
                String csv = String.format("%s,%d,%.2f",
                        element.getSensorId(),
                        element.getTimestamp(),
                        element.getTemperature()
                );
                return csv.getBytes(StandardCharsets.UTF_8);
            }
        };

        // CSV反序列化器
        DeserializationSchema<SensorReading> csvDeserializer = new DeserializationSchema<SensorReading>() {
            @Override
            public SensorReading deserialize(byte[] message) throws IOException {
                String csv = new String(message, StandardCharsets.UTF_8);
                String[] fields = csv.split(",");

                if (fields.length != 3) {
                    throw new IOException("CSV格式错误：" + csv);
                }

                return new SensorReading(
                        fields[0],
                        Long.parseLong(fields[1]),
                        Double.parseDouble(fields[2])
                );
            }

            @Override
            public boolean isEndOfStream(SensorReading nextElement) {
                return false;
            }

            @Override
            public TypeInformation<SensorReading> getProducedType() {
                return TypeInformation.of(SensorReading.class);
            }
        };

        DataStream<String> csvStream = sensorStream.map(
                sensor -> new String(csvSerializer.serialize(sensor), StandardCharsets.UTF_8)
        );

        csvStream.print("CSV格式");

        // ============================================================
        // 4. 类型信息显式指定
        // ============================================================
        System.out.println("\n========== 显式类型信息 ==========");

        // 使用TypeHint指定复杂类型
        DataStream<org.apache.flink.api.java.tuple.Tuple2<String, Double>> tupleStream = sensorStream
                .map(sensor -> new org.apache.flink.api.java.tuple.Tuple2<>(
                        sensor.getSensorId(),
                        sensor.getTemperature()
                ))
                .returns(new TypeHint<org.apache.flink.api.java.tuple.Tuple2<String, Double>>() {
                });

        tupleStream.print("显式类型信息");

        // ============================================================
        // 5. Kryo序列化配置
        // ============================================================
        System.out.println("\n========== Kryo序列化配置 ==========");

        // 注册Kryo序列化的类（提高性能）
        env.getConfig().registerKryoType(SensorReading.class);

        // 禁用Kryo（强制使用POJO序列化）
        // env.getConfig().disableGenericTypes();

        // 启用强制Kryo序列化（不推荐）
        // env.getConfig().enableForceKryo();

        // ============================================================
        // 6. 序列化版本兼容性
        // ============================================================
        System.out.println("\n========== 序列化兼容性 ==========");

        /*
         * POJO字段变更兼容性：
         *
         * 兼容的变更：
         * - 添加新字段（带默认值）
         * - 删除字段（旧数据该字段被忽略）
         *
         * 不兼容的变更：
         * - 修改字段类型
         * - 修改字段名称
         * - 删除字段且无默认值
         *
         * 建议：
         * - 使用Avro支持Schema演化
         * - 版本升级时使用Savepoint
         * - 谨慎修改POJO结构
         */

        env.execute("Flink Serialization Demo");

        /*
         * 序列化总结：
         *
         * 1. 选择建议：
         *    - 优先使用POJO（性能最好）
         *    - 需要Schema演化用Avro
         *    - 跨语言用Protobuf
         *    - 避免使用Kryo（性能差）
         *
         * 2. POJO最佳实践：
         *    - 遵守POJO规范
         *    - 添加serialVersionUID
         *    - 字段使用基本类型或可序列化类型
         *    - 避免使用复杂嵌套
         *
         * 3. 自定义序列化场景：
         *    - Kafka消息格式
         *    - 文件读写格式
         *    - 外部系统交互
         *
         * 4. 性能优化：
         *    - 使用POJO代替通用类型
         *    - 注册常用类到Kryo
         *    - 避免序列化大对象
         *    - 使用二进制格式（Avro/Protobuf）
         *
         * 5. 类型擦除问题：
         *    - Java泛型类型擦除
         *    - 使用TypeHint显式指定
         *    - 使用returns()方法
         *
         * 6. 常见错误：
         *    a. 类型信息缺失：
         *       - 使用returns()指定类型
         *       - 使用TypeInformation.of()
         *
         *    b. 序列化异常：
         *       - 检查POJO规范
         *       - 添加serialVersionUID
         *       - 使用Kryo作为后备
         *
         *    c. 性能问题：
         *       - 避免使用Kryo
         *       - 优化POJO结构
         *       - 减少序列化数据量
         *
         * 7. 监控指标：
         *    - 序列化/反序列化时间
         *    - 序列化后数据大小
         *    - State大小增长
         *
         * 8. Schema演化：
         *    - Avro支持向前/向后兼容
         *    - Protobuf支持字段编号
         *    - POJO需要谨慎修改
         *
         * 9. 生产环境建议：
         *    - 使用POJO或Avro
         *    - 添加版本控制
         *    - 测试兼容性
         *    - 监控序列化性能
         *    - 定期优化数据结构
         *
         * 10. 不同格式对比：
         *
         *     格式      性能  大小  可读性  Schema演化  跨语言
         *     POJO      ★★★★★  ★★★★   ★★      ★★        ★
         *     Avro      ★★★★   ★★★★   ★       ★★★★★     ★★★★
         *     Protobuf  ★★★★★  ★★★★★  ★       ★★★★★     ★★★★★
         *     JSON      ★★     ★★     ★★★★★   ★★★       ★★★★★
         *     Kryo      ★★     ★★★    ★       ★         ★★
         *
         * 推荐：
         * - 内部使用：POJO
         * - 外部交互：Avro或Protobuf
         * - 调试开发：JSON
         */
    }
}

