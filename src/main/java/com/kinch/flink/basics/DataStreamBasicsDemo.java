package com.kinch.flink.basics;

import com.kinch.flink.common.entity.SensorReading;
import com.kinch.flink.common.source.CustomSensorSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink DataStream API 基础操作演示
 * <p>
 * 理论知识：
 * 1. StreamExecutionEnvironment：Flink流处理程序的执行环境
 * 2. DataStream：表示一个不可变的数据流
 * 3. Transformation：数据转换操作（map、filter、keyBy等）
 * 4. Source：数据源，可以是文件、Socket、Kafka等
 * 5. Sink：数据输出目标
 * <p>
 * 核心概念：
 * - Map：一对一转换，将每个元素转换为另一个元素
 * - Filter：过滤操作，根据条件保留或丢弃元素
 * - FlatMap：一对多转换，一个元素可以产生0个、1个或多个元素
 * - KeyBy：按照指定的key对数据流进行分区
 */
public class DataStreamBasicsDemo {

    public static void main(String[] args) throws Exception {

        // ============================================================
        // 1. 创建流处理执行环境
        // ============================================================
        // 获取执行环境，这是所有Flink程序的入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为1，方便观察输出（生产环境根据实际情况设置）
        env.setParallelism(1);

        // ============================================================
        // 2. 从自定义数据源读取数据
        // ============================================================
        DataStream<SensorReading> sensorStream = env.addSource(new CustomSensorSource())
                .name("Custom-Sensor-Source");

        // ============================================================
        // 3. Map转换：提取传感器ID和温度
        // ============================================================
        // Map是一对一的转换操作，将SensorReading转换为Tuple2
        DataStream<Tuple2<String, Double>> mappedStream = sensorStream
                .map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(SensorReading value) throws Exception {
                        return new Tuple2<>(value.getSensorId(), value.getTemperature());
                    }
                })
                .name("Map-Extract-ID-Temperature");

        // 也可以使用Lambda表达式简化（推荐）
        DataStream<String> simpleMappedStream = sensorStream
                .map(sensor -> "传感器: " + sensor.getSensorId() + ", 温度: " + sensor.getTemperature())
                .name("Map-To-String");

        // ============================================================
        // 4. Filter转换：过滤出温度大于22度的数据
        // ============================================================
        // Filter用于根据条件过滤数据
        DataStream<SensorReading> filteredStream = sensorStream
                .filter(new FilterFunction<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return value.getTemperature() > 22.0;
                    }
                })
                .name("Filter-Temperature-Above-22");

        // Lambda表达式简化版本
        DataStream<SensorReading> filteredStreamLambda = sensorStream
                .filter(sensor -> sensor.getTemperature() > 22.0)
                .name("Filter-Lambda");

        // ============================================================
        // 5. FlatMap转换：将一条数据拆分为多条
        // ============================================================
        // FlatMap可以将一个元素转换为0个、1个或多个元素
        DataStream<String> flatMappedStream = sensorStream
                .flatMap((SensorReading sensor, org.apache.flink.util.Collector<String> out) -> {
                    // 如果温度高于20度，输出多条信息
                    if (sensor.getTemperature() > 20.0) {
                        out.collect("高温警告: " + sensor.getSensorId());
                        out.collect("温度值: " + sensor.getTemperature());
                    }
                })
                .returns(String.class)
                .name("FlatMap-Temperature-Warning");

        // ============================================================
        // 6. KeyBy分组：按传感器ID分组
        // ============================================================
        // KeyBy将数据流按照指定的key进行分区，相同key的数据会被分配到同一个分区
        // 这是进行聚合操作的前提
        org.apache.flink.streaming.api.datastream.KeyedStream<SensorReading, String> keyedStream =
                sensorStream.keyBy(sensor -> sensor.getSensorId());

        // ============================================================
        // 7. 聚合操作：计算每个传感器的最大温度
        // ============================================================
        // maxBy会保留具有最大值的完整记录
        DataStream<SensorReading> maxTempStream = keyedStream
                .maxBy("temperature")
                .name("Max-Temperature-By-Sensor");

        // ============================================================
        // 8. Reduce操作：自定义聚合逻辑
        // ============================================================
        // Reduce可以实现自定义的聚合逻辑
        DataStream<SensorReading> reducedStream = keyedStream
                .reduce((sensor1, sensor2) -> {
                    // 保留温度较高的记录，但更新时间戳为最新
                    if (sensor1.getTemperature() > sensor2.getTemperature()) {
                        sensor1.setTimestamp(sensor2.getTimestamp());
                        return sensor1;
                    } else {
                        return sensor2;
                    }
                })
                .name("Reduce-Max-Temperature");

        // ============================================================
        // 9. Union操作：合并多个数据流
        // ============================================================
        // Union可以合并多个类型相同的数据流
        DataStream<SensorReading> unionStream = filteredStream
                .union(sensorStream)
                .map(x -> x)
                .name("Union-Streams");

        // ============================================================
        // 10. Connect操作：连接两个不同类型的数据流
        // ============================================================
        // Connect可以连接两个类型不同的数据流，并对它们分别处理
        DataStream<String> alertStream = sensorStream
                .connect(simpleMappedStream)
                .map(new org.apache.flink.streaming.api.functions.co.CoMapFunction<SensorReading, String, String>() {
                    @Override
                    public String map1(SensorReading value) throws Exception {
                        return "传感器数据: " + value.toString();
                    }

                    @Override
                    public String map2(String value) throws Exception {
                        return "字符串数据: " + value;
                    }
                })
                .name("Connect-And-CoMap");

        // ============================================================
        // 11. 输出结果
        // ============================================================
        // 打印原始数据流
        sensorStream.print("原始传感器数据");

        // 打印过滤后的数据
        filteredStream.print("高温传感器(>22度)");

        // 打印每个传感器的最大温度
        maxTempStream.print("最大温度记录");

        // ============================================================
        // 12. 执行任务
        // ============================================================
        // 注意：Flink程序是惰性执行的，只有调用execute()才会真正执行
        env.execute("Flink DataStream Basics Demo");

        /*
         * 执行流程说明：
         * 1. 定义Source：从CustomSensorSource读取数据
         * 2. 定义Transformation：map、filter、keyBy等转换操作
         * 3. 定义Sink：print输出到控制台
         * 4. 调用execute()：提交任务到JobManager执行
         *
         * 并行度说明：
         * - 每个Operator可以有多个并行实例（Task）
         * - KeyBy会根据key的hash值将数据分配到不同的分区
         * - 相同key的数据会被发送到同一个Task处理
         */
    }
}

