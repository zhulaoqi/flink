package com.kinch.flink.window;

import com.kinch.flink.common.entity.SensorReading;
import com.kinch.flink.common.source.CustomSensorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Flink 窗口（Window）基础演示
 * <p>
 * 理论知识：
 * 1. 窗口概念：将无界的数据流切分成有界的数据块进行处理
 * 2. 窗口类型：
 * - 时间窗口（Time Window）：基于时间切分
 * a. 滚动时间窗口（Tumbling）：固定大小，窗口之间不重叠
 * b. 滑动时间窗口（Sliding）：固定大小，窗口之间可以重叠
 * c. 会话窗口（Session）：根据活动间隔动态创建窗口
 * - 计数窗口（Count Window）：基于元素数量切分
 * a. 滚动计数窗口
 * b. 滑动计数窗口
 * <p>
 * 3. 时间语义：
 * - 处理时间（Processing Time）：机器系统时间
 * - 事件时间（Event Time）：数据自带的时间戳
 * - 摄入时间（Ingestion Time）：数据进入Flink的时间
 * <p>
 * 4. 窗口函数：
 * - ReduceFunction：增量聚合，来一条处理一条
 * - AggregateFunction：增量聚合，更灵活
 * - ProcessWindowFunction：全量聚合，可以访问窗口元数据
 */
public class WindowBasicsDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取传感器数据
        DataStream<SensorReading> sensorStream = env.addSource(new CustomSensorSource());

        // ============================================================
        // 1. 滚动时间窗口（Tumbling Time Window）
        // ============================================================
        // 每10秒统计一次各传感器的平均温度
        // 窗口大小固定为10秒，窗口之间不重叠
        // [0-10), [10-20), [20-30) ...
        DataStream<SensorReading> tumblingWindowResult = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce((sensor1, sensor2) -> {
                    // 简单计算平均温度（实际应该用AggregateFunction更准确）
                    sensor1.setTemperature((sensor1.getTemperature() + sensor2.getTemperature()) / 2);
                    return sensor1;
                });

        tumblingWindowResult.print("滚动窗口-10秒平均温度");

        // ============================================================
        // 2. 滑动时间窗口（Sliding Time Window）
        // ============================================================
        // 窗口大小15秒，每5秒滑动一次
        // 窗口之间有重叠：[0-15), [5-20), [10-25) ...
        // 适用于：需要频繁更新结果的场景，如实时趋势分析
        DataStream<SensorReading> slidingWindowResult = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(5)))
                .reduce((sensor1, sensor2) -> {
                    sensor1.setTemperature(Math.max(sensor1.getTemperature(), sensor2.getTemperature()));
                    return sensor1;
                });

        slidingWindowResult.print("滑动窗口-15秒窗口5秒滑动最大温度");

        // ============================================================
        // 3. 会话窗口（Session Window）
        // ============================================================
        // 当连续两次事件的时间间隔超过gap时，会创建新窗口
        // 适用于：用户会话分析，活动模式检测
        DataStream<SensorReading> sessionWindowResult = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .reduce((sensor1, sensor2) -> {
                    // 统计会话期间的数据条数（简化版）
                    sensor1.setTemperature(sensor1.getTemperature() + 1);
                    return sensor1;
                });

        sessionWindowResult.print("会话窗口-5秒gap");

        // ============================================================
        // 4. 滚动计数窗口（Tumbling Count Window）
        // ============================================================
        // 每收集5条数据触发一次计算
        DataStream<SensorReading> countWindowResult = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .countWindow(5)
                .reduce((sensor1, sensor2) -> {
                    sensor1.setTemperature((sensor1.getTemperature() + sensor2.getTemperature()) / 2);
                    return sensor1;
                });

        countWindowResult.print("滚动计数窗口-每5条数据");

        // ============================================================
        // 5. 滑动计数窗口（Sliding Count Window）
        // ============================================================
        // 窗口大小10条，每5条数据滑动一次
        // 窗口之间有重叠
        DataStream<SensorReading> slidingCountWindowResult = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .countWindow(10, 5)
                .reduce((sensor1, sensor2) -> {
                    sensor1.setTemperature(Math.max(sensor1.getTemperature(), sensor2.getTemperature()));
                    return sensor1;
                });

        slidingCountWindowResult.print("滑动计数窗口-窗口10条滑动5条");

        // ============================================================
        // 6. 使用AggregateFunction进行增量聚合
        // ============================================================
        // AggregateFunction更加灵活，可以自定义累加器
        DataStream<Tuple2<String, Double>> avgTempResult = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Tuple2<String, Double>>() {

                    // 创建累加器：(温度总和, 数据条数)
                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<>(0.0, 0);
                    }

                    // 添加元素到累加器
                    @Override
                    public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
                        return new Tuple2<>(
                                accumulator.f0 + value.getTemperature(),
                                accumulator.f1 + 1
                        );
                    }

                    // 获取最终结果：计算平均温度
                    @Override
                    public Tuple2<String, Double> getResult(Tuple2<Double, Integer> accumulator) {
                        return new Tuple2<>("平均温度", accumulator.f0 / accumulator.f1);
                    }

                    // 合并两个累加器（用于会话窗口）
                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                    }
                });

        avgTempResult.print("增量聚合-10秒平均温度");

        // ============================================================
        // 7. 使用ProcessWindowFunction访问窗口元数据
        // ============================================================
        // ProcessWindowFunction可以访问窗口的开始时间、结束时间等元数据
        DataStream<String> windowMetadataResult = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
                        SensorReading, String, String, TimeWindow>() {

                    @Override
                    public void process(String key, Context context,
                                        Iterable<SensorReading> elements,
                                        org.apache.flink.util.Collector<String> out) throws Exception {

                        // 获取窗口信息
                        long windowStart = context.window().getStart();
                        long windowEnd = context.window().getEnd();

                        // 计算窗口内的数据统计
                        int count = 0;
                        double sum = 0.0;
                        for (SensorReading sensor : elements) {
                            count++;
                            sum += sensor.getTemperature();
                        }
                        double avg = sum / count;

                        String result = String.format(
                                "传感器: %s | 窗口: [%d - %d] | 数据条数: %d | 平均温度: %.2f",
                                key, windowStart, windowEnd, count, avg
                        );

                        out.collect(result);
                    }
                });

        windowMetadataResult.print("窗口元数据");

        // ============================================================
        // 8. 增量聚合 + 全量聚合组合使用
        // ============================================================
        // 先用AggregateFunction做增量聚合，再用ProcessWindowFunction访问窗口信息
        // 这样既高效又能获取窗口元数据
        DataStream<String> combinedResult = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        // AggregateFunction：增量聚合计算平均值
                        new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>() {
                            @Override
                            public Tuple2<Double, Integer> createAccumulator() {
                                return new Tuple2<>(0.0, 0);
                            }

                            @Override
                            public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> acc) {
                                return new Tuple2<>(acc.f0 + value.getTemperature(), acc.f1 + 1);
                            }

                            @Override
                            public Double getResult(Tuple2<Double, Integer> acc) {
                                return acc.f0 / acc.f1;
                            }

                            @Override
                            public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                                return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                            }
                        },
                        // ProcessWindowFunction：包装结果并添加窗口信息
                        new org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
                                Double, String, String, TimeWindow>() {

                            @Override
                            public void process(String key, Context context, Iterable<Double> elements,
                                                org.apache.flink.util.Collector<String> out) throws Exception {
                                Double avgTemp = elements.iterator().next();
                                long windowEnd = context.window().getEnd();
                                out.collect(String.format("传感器: %s | 窗口结束: %d | 平均温度: %.2f",
                                        key, windowEnd, avgTemp));
                            }
                        }
                );

        combinedResult.print("增量+全量组合");

        env.execute("Flink Window Basics Demo");

        /*
         * 窗口机制总结：
         *
         * 1. 窗口分配器（Window Assigner）：
         *    - 决定数据属于哪个窗口
         *    - 一条数据可能属于多个窗口（滑动窗口）
         *
         * 2. 窗口触发器（Trigger）：
         *    - 决定何时触发窗口计算
         *    - 默认：窗口结束时间到达且有数据时触发
         *
         * 3. 窗口函数（Window Function）：
         *    - 增量聚合：ReduceFunction, AggregateFunction（推荐）
         *    - 全量聚合：ProcessWindowFunction
         *    - 组合使用：增量+全量（最佳实践）
         *
         * 4. 窗口驱逐器（Evictor）：
         *    - 可选，用于在窗口计算前后移除元素
         *
         * 5. 选择建议：
         *    - 固定时间统计 → 滚动时间窗口
         *    - 实时趋势分析 → 滑动时间窗口
         *    - 用户会话分析 → 会话窗口
         *    - 固定数量批处理 → 计数窗口
         */
    }
}

