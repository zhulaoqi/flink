package com.kinch.flink.state;

import com.kinch.flink.common.entity.SensorReading;
import com.kinch.flink.common.source.CustomSensorSource;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink 状态管理（State Management）演示
 * <p>
 * 理论知识：
 * 1. 状态概念：
 * - 状态是Flink中的内存数据，用于存储中间计算结果
 * - 状态可以被持久化到StateBackend
 * - 状态是容错的基础（通过Checkpoint机制）
 * <p>
 * 2. 状态分类：
 * a. Keyed State（键控状态）：
 * - 只能在KeyedStream上使用
 * - 每个key有独立的状态
 * - 类型：ValueState、ListState、MapState、ReducingState、AggregatingState
 * <p>
 * b. Operator State（算子状态）：
 * - 与key无关，属于整个算子实例
 * - 类型：ListState、UnionListState、BroadcastState
 * <p>
 * 3. Keyed State详解：
 * - ValueState<T>：保存单个值
 * - ListState<T>：保存元素列表
 * - MapState<UK, UV>：保存键值对映射
 * - ReducingState<T>：保存单个值，代表所有元素的聚合结果
 * - AggregatingState<IN, OUT>：保存单个值，更灵活的聚合
 * <p>
 * 4. State TTL（生存时间）：
 * - 防止状态无限增长
 * - 自动清理过期状态
 * - 可配置更新策略和可见性
 * <p>
 * 5. StateBackend（状态后端）：
 * - MemoryStateBackend：内存存储（适合测试）
 * - FsStateBackend：内存+文件系统（适合中等状态）
 * - RocksDBStateBackend：RocksDB（适合大状态）
 */
public class StateManagementDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取传感器数据
        DataStream<SensorReading> sensorStream = env.addSource(new CustomSensorSource());

        // ============================================================
        // 1. ValueState：保存单个值
        // ============================================================
        // 使用场景：记录每个传感器的上一次温度，检测温度变化
        DataStream<Tuple2<String, Double>> temperatureChangeStream = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .flatMap(new TemperatureChangeDetector());

        temperatureChangeStream.print("温度变化检测");

        // ============================================================
        // 2. ListState：保存元素列表
        // ============================================================
        // 使用场景：保存每个传感器最近3次的温度读数
        DataStream<String> recentReadingsStream = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .flatMap(new RecentReadingsKeeper(3));

        recentReadingsStream.print("最近3次读数");

        // ============================================================
        // 3. MapState：保存键值对映射
        // ============================================================
        // 使用场景：统计每个传感器在不同温度区间的出现次数
        DataStream<String> temperatureDistribution = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .flatMap(new TemperatureDistributionCounter());

        temperatureDistribution.print("温度分布统计");

        // ============================================================
        // 4. ReducingState：聚合状态
        // ============================================================
        // 使用场景：计算每个传感器的最高温度
        DataStream<Tuple2<String, Double>> maxTemperatureStream = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .map(new MaxTemperatureTracker());

        maxTemperatureStream.print("最高温度跟踪");

        // ============================================================
        // 5. AggregatingState：更灵活的聚合状态
        // ============================================================
        // 使用场景：计算每个传感器的平均温度
        DataStream<Tuple2<String, Double>> avgTemperatureStream = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .flatMap(new AverageTemperatureCalculator());

        avgTemperatureStream.print("平均温度计算");

        // ============================================================
        // 6. State TTL：状态生存时间
        // ============================================================
        // 使用场景：自动清理长时间未更新的状态
        DataStream<String> stateWithTTLStream = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .flatMap(new StateWithTTLExample());

        stateWithTTLStream.print("带TTL的状态");

        env.execute("Flink State Management Demo");

        /*
         * 状态管理最佳实践：
         *
         * 1. 状态类型选择：
         *    - 单值累加 → ValueState
         *    - 保存历史记录 → ListState
         *    - 复杂统计 → MapState
         *    - 简单聚合 → ReducingState
         *    - 复杂聚合 → AggregatingState
         *
         * 2. 状态清理：
         *    - 必须设置State TTL，防止状态无限增长
         *    - 关键业务可以手动清理状态
         *
         * 3. 状态后端选择：
         *    - 小状态（几百MB）→ FsStateBackend
         *    - 大状态（几GB以上）→ RocksDBStateBackend
         *    - 永远不要用MemoryStateBackend在生产环境
         *
         * 4. 性能优化：
         *    - 减少状态访问次数
         *    - 使用ReducingState代替ListState + reduce
         *    - RocksDB调优：增加block cache、使用SSD
         *
         * 5. 监控指标：
         *    - 状态大小
         *    - Checkpoint大小和时长
         *    - 状态访问延迟
         */
    }

    /**
     * 温度变化检测器 - 使用ValueState
     */
    public static class TemperatureChangeDetector extends RichFlatMapFunction<SensorReading, Tuple2<String, Double>> {

        // ValueState：保存上一次的温度
        private transient ValueState<Double> lastTemperatureState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 定义状态描述符
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                    "last-temperature",  // 状态名称
                    Double.class         // 状态类型
            );

            // 可选：设置State TTL（1小时）
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 创建和写入时更新TTL
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 过期后不可见
                    .build();
            descriptor.enableTimeToLive(ttlConfig);

            // 获取状态句柄
            lastTemperatureState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple2<String, Double>> out) throws Exception {
            // 获取上一次温度
            Double lastTemp = lastTemperatureState.value();

            if (lastTemp != null) {
                // 计算温度变化
                double change = value.getTemperature() - lastTemp;

                // 如果温度变化超过1度，输出警报
                if (Math.abs(change) > 1.0) {
                    out.collect(new Tuple2<>(
                            value.getSensorId() + " 温度变化警报",
                            change
                    ));
                }
            }

            // 更新状态
            lastTemperatureState.update(value.getTemperature());
        }
    }

    /**
     * 最近读数保持器 - 使用ListState
     */
    public static class RecentReadingsKeeper extends RichFlatMapFunction<SensorReading, String> {

        private final int maxSize;
        private transient ListState<Double> recentReadingsState;

        public RecentReadingsKeeper(int maxSize) {
            this.maxSize = maxSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<Double> descriptor = new ListStateDescriptor<>(
                    "recent-readings",
                    Double.class
            );
            recentReadingsState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void flatMap(SensorReading value, Collector<String> out) throws Exception {
            // 获取当前列表
            List<Double> readings = new ArrayList<>();
            for (Double reading : recentReadingsState.get()) {
                readings.add(reading);
            }

            // 添加新读数
            readings.add(value.getTemperature());

            // 保持最多maxSize个元素
            if (readings.size() > maxSize) {
                readings.remove(0);
            }

            // 更新状态
            recentReadingsState.update(readings);

            // 输出
            out.collect(String.format(
                    "传感器: %s, 最近%d次读数: %s",
                    value.getSensorId(), readings.size(), readings
            ));
        }
    }

    /**
     * 温度分布计数器 - 使用MapState
     */
    public static class TemperatureDistributionCounter extends RichFlatMapFunction<SensorReading, String> {

        // MapState<温度区间, 出现次数>
        private transient MapState<String, Long> distributionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>(
                    "temperature-distribution",
                    String.class,
                    Long.class
            );
            distributionState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void flatMap(SensorReading value, Collector<String> out) throws Exception {
            // 确定温度区间
            String range = getTemperatureRange(value.getTemperature());

            // 获取当前计数
            Long count = distributionState.get(range);
            if (count == null) {
                count = 0L;
            }

            // 更新计数
            distributionState.put(range, count + 1);

            // 输出所有区间的统计
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("传感器: %s 温度分布 - ", value.getSensorId()));
            for (String key : distributionState.keys()) {
                sb.append(String.format("%s: %d次; ", key, distributionState.get(key)));
            }

            out.collect(sb.toString());
        }

        private String getTemperatureRange(double temperature) {
            if (temperature < 20) return "低温(<20)";
            else if (temperature < 25) return "适中(20-25)";
            else return "高温(>=25)";
        }
    }

    /**
     * 最高温度跟踪器 - 使用ReducingState
     */
    public static class MaxTemperatureTracker extends RichMapFunction<SensorReading, Tuple2<String, Double>> {

        private transient ReducingState<Double> maxTemperatureState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ReducingStateDescriptor<Double> descriptor = new ReducingStateDescriptor<>(
                    "max-temperature",
                    (temp1, temp2) -> Math.max(temp1, temp2),  // 归约函数：取最大值
                    Double.class
            );
            maxTemperatureState = getRuntimeContext().getReducingState(descriptor);
        }

        @Override
        public Tuple2<String, Double> map(SensorReading value) throws Exception {
            // 添加新温度（自动归约）
            maxTemperatureState.add(value.getTemperature());

            // 获取最大温度
            Double maxTemp = maxTemperatureState.get();

            return new Tuple2<>(value.getSensorId() + " 历史最高温度", maxTemp);
        }
    }

    /**
     * 平均温度计算器 - 使用AggregatingState
     */
    public static class AverageTemperatureCalculator extends RichFlatMapFunction<SensorReading, Tuple2<String, Double>> {

        private transient AggregatingState<Double, Double> avgTemperatureState;

        @Override
        public void open(Configuration parameters) throws Exception {
            AggregatingStateDescriptor<Double, Tuple2<Double, Long>, Double> descriptor =
                    new AggregatingStateDescriptor<>(
                            "avg-temperature",
                            new org.apache.flink.api.common.functions.AggregateFunction<Double, Tuple2<Double, Long>, Double>() {

                                @Override
                                public Tuple2<Double, Long> createAccumulator() {
                                    return new Tuple2<>(0.0, 0L);  // (总和, 数量)
                                }

                                @Override
                                public Tuple2<Double, Long> add(Double value, Tuple2<Double, Long> accumulator) {
                                    return new Tuple2<>(
                                            accumulator.f0 + value,
                                            accumulator.f1 + 1
                                    );
                                }

                                @Override
                                public Double getResult(Tuple2<Double, Long> accumulator) {
                                    return accumulator.f0 / accumulator.f1;
                                }

                                @Override
                                public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
                                    return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                                }
                            },
                            TypeInformation.of(new TypeHint<Tuple2<Double, Long>>() {
                            })
                    );

            avgTemperatureState = getRuntimeContext().getAggregatingState(descriptor);
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple2<String, Double>> out) throws Exception {
            // 添加新温度（自动聚合）
            avgTemperatureState.add(value.getTemperature());

            // 获取平均温度
            Double avgTemp = avgTemperatureState.get();

            out.collect(new Tuple2<>(value.getSensorId() + " 平均温度", avgTemp));
        }
    }

    /**
     * 带TTL的状态示例
     */
    public static class StateWithTTLExample extends RichFlatMapFunction<SensorReading, String> {

        private transient ValueState<Tuple3<Long, Double, Long>> sensorInfoState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 状态类型：(首次时间戳, 累计温度, 数据条数)
            ValueStateDescriptor<Tuple3<Long, Double, Long>> descriptor =
                    new ValueStateDescriptor<>(
                            "sensor-info-with-ttl",
                            TypeInformation.of(new TypeHint<Tuple3<Long, Double, Long>>() {
                            })
                    );

            // 配置TTL：10分钟
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.minutes(10))
                    // 更新策略：创建和写入时更新TTL
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    // 可见性：过期后不返回
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    // 清理策略：在读取时清理过期状态
                    .cleanupIncrementally(10, true)
                    .build();

            descriptor.enableTimeToLive(ttlConfig);

            sensorInfoState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(SensorReading value, Collector<String> out) throws Exception {
            Tuple3<Long, Double, Long> info = sensorInfoState.value();

            if (info == null) {
                // 首次访问
                info = new Tuple3<>(
                        value.getTimestamp(),
                        value.getTemperature(),
                        1L
                );
                out.collect(String.format(
                        "传感器 %s 首次记录，温度: %.2f",
                        value.getSensorId(), value.getTemperature()
                ));
            } else {
                // 更新信息
                info.f1 += value.getTemperature();
                info.f2 += 1;

                double avgTemp = info.f1 / info.f2;
                long duration = (value.getTimestamp() - info.f0) / 1000;  // 秒

                out.collect(String.format(
                        "传感器 %s - 持续时间: %d秒, 平均温度: %.2f, 数据条数: %d (状态将在10分钟无更新后自动清理)",
                        value.getSensorId(), duration, avgTemp, info.f2
                ));
            }

            sensorInfoState.update(info);
        }
    }
}

