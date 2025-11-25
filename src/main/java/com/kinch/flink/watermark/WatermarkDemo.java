package com.kinch.flink.watermark;

import com.kinch.flink.common.entity.SensorReading;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.Random;

/**
 * Flink 水位线（Watermark）演示
 * <p>
 * 理论知识：
 * 1. Watermark概念：
 * - 水位线是一个时间戳，表示早于该时间戳的事件都已到达
 * - 用于处理乱序数据和延迟数据
 * - Watermark = 当前最大事件时间 - 最大延迟时间
 * <p>
 * 2. 为什么需要Watermark：
 * - 现实中数据到达可能乱序或延迟
 * - 需要一种机制判断何时触发窗口计算
 * - 在完整性和延迟之间做权衡
 * <p>
 * 3. Watermark生成策略：
 * - 周期性生成（Periodic）：每隔一段时间生成一次，适合规律性数据
 * - 标记式生成（Punctuated）：根据特定事件生成，适合不规律数据
 * <p>
 * 4. Watermark传播：
 * - 单流：向下游传播
 * - 多流：取所有输入流的最小值（木桶原理）
 * <p>
 * 5. 延迟数据处理：
 * - allowedLateness：允许窗口延迟关闭，在此期间到达的数据仍能参与计算
 * - sideOutputLateData：将迟到数据输出到侧输出流
 */
public class WatermarkDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ============================================================
        // 1. 生成带有乱序的传感器数据
        // ============================================================
        DataStream<SensorReading> sensorStream = env.addSource(new OutOfOrderSensorSource());

        // ============================================================
        // 2. 使用内置的单调递增Watermark策略
        // ============================================================
        // 适用于：事件时间单调递增的场景（无乱序）
        DataStream<SensorReading> streamWithMonotoneWatermark = sensorStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SensorReading>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );

        // ============================================================
        // 3. 使用有界乱序Watermark策略（常用）
        // ============================================================
        // 适用于：数据有一定程度的乱序，但乱序程度有上界
        // maxOutOfOrderness：允许的最大乱序时间
        DataStream<SensorReading> streamWithBoundedWatermark = sensorStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SensorReading>forBoundedOutOfOrderness(
                                        java.time.Duration.ofSeconds(5)  // 允许5秒的乱序
                                )
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );

        System.out.println("使用有界乱序Watermark策略，允许5秒乱序");

        // ============================================================
        // 4. 自定义Watermark生成器（周期性）
        // ============================================================
        DataStream<SensorReading> streamWithCustomWatermark = sensorStream
                .assignTimestampsAndWatermarks(
                        new WatermarkStrategy<SensorReading>() {
                            @Override
                            public WatermarkGenerator<SensorReading> createWatermarkGenerator(
                                    WatermarkGeneratorSupplier.Context context) {
                                return new CustomPeriodicWatermarkGenerator();
                            }

                            @Override
                            public TimestampAssigner<SensorReading> createTimestampAssigner(
                                    TimestampAssignerSupplier.Context context) {
                                return (event, timestamp) -> event.getTimestamp();
                            }
                        }
                );

        // ============================================================
// 5. 基于事件时间的窗口计算（配合Watermark）
// ============================================================
// 先定义 OutputTag（必须在两处使用同一个实例）
        OutputTag<SensorReading> lateDataOutputTag =
                new OutputTag<SensorReading>("late-data") {
                };

        SingleOutputStreamOperator<SensorReading> windowResult = streamWithBoundedWatermark
                .keyBy(sensor -> sensor.getSensorId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 允许窗口延迟2秒关闭
                .allowedLateness(Time.seconds(2))
                // 将迟到数据输出到侧输出流（使用同一个实例）
                .sideOutputLateData(lateDataOutputTag)
                .reduce((sensor1, sensor2) -> {
                    sensor1.setTemperature(Math.max(sensor1.getTemperature(), sensor2.getTemperature()));
                    return sensor1;
                });

        windowResult.print("窗口计算结果");

        // 获取迟到数据（使用同一个实例）
        DataStream<SensorReading> lateData = windowResult.getSideOutput(lateDataOutputTag);
        lateData.print("迟到数据");


        // ============================================================
        // 6. 演示Watermark对窗口触发的影响
        // ============================================================
        streamWithBoundedWatermark
                .keyBy(sensor -> sensor.getSensorId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
                        SensorReading, String, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow>() {

                    @Override
                    public void process(String key, Context context,
                                        Iterable<SensorReading> elements,
                                        org.apache.flink.util.Collector<String> out) throws Exception {

                        long windowStart = context.window().getStart();
                        long windowEnd = context.window().getEnd();
                        long currentWatermark = context.currentWatermark();

                        int count = 0;
                        for (SensorReading sensor : elements) {
                            count++;
                        }

                        String result = String.format(
                                "传感器: %s | 窗口: [%d - %d] | 当前Watermark: %d | 数据条数: %d | 说明: Watermark到达窗口结束时间时触发计算",
                                key, windowStart, windowEnd, currentWatermark, count
                        );

                        out.collect(result);
                    }
                })
                .print("Watermark触发机制演示");

        env.execute("Flink Watermark Demo");

        /*
         * Watermark工作原理：
         *
         * 1. 数据到达流程：
         *    事件 → 提取时间戳 → 生成Watermark → 窗口计算
         *
         * 2. 窗口触发条件：
         *    Watermark >= 窗口结束时间 → 触发窗口计算
         *
         * 3. 延迟数据处理策略：
         *    a. 不处理：直接丢弃（默认）
         *    b. allowedLateness：延迟窗口关闭时间
         *    c. sideOutput：输出到侧输出流单独处理
         *
         * 4. Watermark生成时机：
         *    - 周期性：默认200ms生成一次（可配置）
         *    - 标记式：每条数据都可能生成Watermark
         *
         * 5. 多并行度下的Watermark：
         *    - 每个并行子任务独立生成Watermark
         *    - 下游任务取所有上游的最小Watermark（木桶效应）
         *    - 某个上游慢会拖累整体进度
         *
         * 6. 生产环境建议：
         *    - 根据业务容忍度设置合理的maxOutOfOrderness
         *    - 对关键指标使用allowedLateness + sideOutput
         *    - 监控Watermark延迟指标
         */
    }

    /**
     * 自定义周期性Watermark生成器
     */
    public static class CustomPeriodicWatermarkGenerator implements WatermarkGenerator<SensorReading> {

        // 允许的最大乱序时间：3秒
        private final long maxOutOfOrderness = 3000L;

        // 当前观察到的最大时间戳
        private long currentMaxTimestamp = Long.MIN_VALUE;

        /**
         * 每条数据都会调用此方法
         */
        @Override
        public void onEvent(SensorReading event, long eventTimestamp, WatermarkOutput output) {
            // 更新最大时间戳
            currentMaxTimestamp = Math.max(currentMaxTimestamp, event.getTimestamp());

            // 可以在这里输出日志观察
            System.out.println(String.format(
                    "[Watermark生成器] 传感器: %s, 事件时间: %d, 当前最大时间: %d",
                    event.getSensorId(), event.getTimestamp(), currentMaxTimestamp
            ));
        }

        /**
         * 周期性调用此方法生成Watermark（默认200ms一次）
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // Watermark = 当前最大时间戳 - 最大乱序时间
            long watermarkTimestamp = currentMaxTimestamp - maxOutOfOrderness;
            output.emitWatermark(new Watermark(watermarkTimestamp));

            System.out.println(String.format(
                    "[Watermark发射] 当前Watermark: %d (最大时间: %d - 乱序时间: %d)",
                    watermarkTimestamp, currentMaxTimestamp, maxOutOfOrderness
            ));
        }
    }

    /**
     * 生成乱序传感器数据的Source
     */
    public static class OutOfOrderSensorSource implements SourceFunction<SensorReading> {

        private volatile boolean running = true;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            long baseTime = System.currentTimeMillis();

            while (running) {
                for (int i = 1; i <= 3; i++) {
                    // 模拟乱序：随机延迟0-10秒
                    long delay = random.nextInt(10) * 1000L;
                    long timestamp = baseTime - delay;

                    double temperature = 20 + random.nextDouble() * 10;

                    SensorReading reading = new SensorReading(
                            "sensor_" + i,
                            timestamp,
                            temperature
                    );

                    ctx.collect(reading);

                    System.out.println(String.format(
                            "[数据源] 生成数据 - 传感器: sensor_%d, 时间戳: %d, 延迟: %d秒",
                            i, timestamp, delay / 1000
                    ));
                }

                baseTime += 1000;  // 基准时间前进1秒
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

