package com.kinch.flink.processfunction;

import com.kinch.flink.common.entity.SensorReading;
import com.kinch.flink.common.source.CustomSensorSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink ProcessFunction演示
 * <p>
 * 理论知识：
 * 1. ProcessFunction概念：
 * - Flink提供的最底层、最灵活的处理函数
 * - 可以访问时间戳、Watermark、定时器等底层信息
 * - 是实现复杂业务逻辑的基础
 * <p>
 * 2. ProcessFunction类型：
 * - ProcessFunction：处理DataStream
 * - KeyedProcessFunction：处理KeyedStream，可以使用定时器
 * - CoProcessFunction：处理连接的两个流
 * - ProcessWindowFunction：处理窗口数据
 * - ProcessAllWindowFunction：处理全局窗口数据
 * <p>
 * 3. 核心功能：
 * - 访问事件时间和处理时间
 * - 注册定时器（Timer）
 * - 访问和更新状态
 * - 发送数据到侧输出流
 * <p>
 * 4. 定时器（Timer）：
 * - 事件时间定时器：基于Watermark触发
 * - 处理时间定时器：基于系统时间触发
 * - 每个key可以注册多个定时器
 * - 定时器会被Checkpoint
 * <p>
 * 5. 使用场景：
 * - 复杂的事件模式检测
 * - 需要定时触发的操作
 * - 需要访问底层信息的场景
 * - 实现自定义窗口逻辑
 */
public class ProcessFunctionDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> sensorStream = env.addSource(new CustomSensorSource());

        // ============================================================
        // 1. ProcessFunction基础使用
        // ============================================================
        // ProcessFunction可以访问时间戳和执行环境信息
        DataStream<String> processedStream = sensorStream
                .process(new ProcessFunction<SensorReading, String>() {

                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<String> out)
                            throws Exception {

                        // 获取当前事件的时间戳
                        Long timestamp = ctx.timestamp();

                        // 获取当前处理时间
                        long processingTime = ctx.timerService().currentProcessingTime();

                        // 获取当前Watermark
                        long watermark = ctx.timerService().currentWatermark();

                        String result = String.format(
                                "[ProcessFunction] 传感器: %s | 温度: %.2f | 事件时间: %d | 处理时间: %d | Watermark: %d",
                                value.getSensorId(), value.getTemperature(),
                                timestamp, processingTime, watermark
                        );

                        out.collect(result);
                    }
                });

        processedStream.print("基础ProcessFunction");

        // ============================================================
        // 2. KeyedProcessFunction：温度持续上升告警
        // ============================================================
        // 场景：如果温度连续10秒持续上升，发出告警
        DataStream<String> temperatureAlertStream = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .process(new TemperatureIncreaseAlertFunction(10));

        temperatureAlertStream.print("温度上升告警");

        // ============================================================
        // 3. KeyedProcessFunction：超时检测
        // ============================================================
        // 场景：如果10秒内没有收到数据，发出超时告警
        DataStream<String> timeoutAlertStream = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .process(new TimeoutAlertFunction(10));

        timeoutAlertStream.print("超时告警");

        // ============================================================
        // 4. KeyedProcessFunction：温度异常检测
        // ============================================================
        // 场景：温度突然升高超过5度，立即告警；如果1分钟内恢复正常则取消告警
        DataStream<String> anomalyDetectionStream = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .process(new TemperatureAnomalyDetector(5.0, 60));

        anomalyDetectionStream.print("异常检测");

        env.execute("Flink ProcessFunction Demo");

        /*
         * ProcessFunction核心方法：
         *
         * 1. processElement()：
         *    - 处理每个元素
         *    - 可以输出0个、1个或多个结果
         *
         * 2. onTimer()：
         *    - 定时器触发时调用
         *    - 可以区分事件时间和处理时间定时器
         *
         * 3. Context对象提供的能力：
         *    - timestamp()：获取当前元素的时间戳
         *    - timerService()：获取定时器服务
         *    - output()：发送数据到侧输出流
         *
         * 4. TimerService提供的能力：
         *    - currentProcessingTime()：当前处理时间
         *    - currentWatermark()：当前Watermark
         *    - registerProcessingTimeTimer()：注册处理时间定时器
         *    - registerEventTimeTimer()：注册事件时间定时器
         *    - deleteProcessingTimeTimer()：删除处理时间定时器
         *    - deleteEventTimeTimer()：删除事件时间定时器
         *
         * 定时器使用注意事项：
         *
         * 1. 定时器会被Checkpoint，具有容错性
         * 2. 相同时间的定时器只会触发一次（去重）
         * 3. 定时器在Watermark到达时触发（事件时间）
         * 4. 处理时间定时器基于系统时间触发
         * 5. 删除定时器时必须提供精确的触发时间
         */
    }

    /**
     * 温度持续上升告警函数
     * 如果温度连续上升超过指定秒数，发出告警
     */
    public static class TemperatureIncreaseAlertFunction
            extends KeyedProcessFunction<String, SensorReading, String> {

        private final int intervalSeconds;

        // 状态：上一次的温度
        private transient ValueState<Double> lastTempState;
        // 状态：上升趋势开始的时间
        private transient ValueState<Long> increaseStartTimeState;
        // 状态：定时器时间
        private transient ValueState<Long> timerState;

        public TemperatureIncreaseAlertFunction(int intervalSeconds) {
            this.intervalSeconds = intervalSeconds;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-temp", Double.class)
            );
            increaseStartTimeState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("increase-start-time", Long.class)
            );
            timerState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("timer", Long.class)
            );
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out)
                throws Exception {

            Double lastTemp = lastTempState.value();
            Long increaseStartTime = increaseStartTimeState.value();
            Long currentTimer = timerState.value();

            // 更新上一次温度
            lastTempState.update(value.getTemperature());

            if (lastTemp != null) {
                if (value.getTemperature() > lastTemp) {
                    // 温度上升
                    if (increaseStartTime == null) {
                        // 开始记录上升趋势
                        increaseStartTime = ctx.timerService().currentProcessingTime();
                        increaseStartTimeState.update(increaseStartTime);

                        // 注册定时器：intervalSeconds秒后触发
                        long timerTime = increaseStartTime + intervalSeconds * 1000L;
                        ctx.timerService().registerProcessingTimeTimer(timerTime);
                        timerState.update(timerTime);

                        System.out.println(String.format(
                                "[定时器注册] 传感器: %s | 注册时间: %d | 触发时间: %d",
                                value.getSensorId(), increaseStartTime, timerTime
                        ));
                    }
                } else {
                    // 温度下降，清除状态和定时器
                    if (currentTimer != null) {
                        ctx.timerService().deleteProcessingTimeTimer(currentTimer);
                        System.out.println(String.format(
                                "[定时器取消] 传感器: %s | 温度下降，取消告警",
                                value.getSensorId()
                        ));
                    }
                    increaseStartTimeState.clear();
                    timerState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {

            // 定时器触发，说明温度持续上升了指定时间
            out.collect(String.format(
                    "告警：传感器 %s 温度持续上升超过 %d 秒！当前温度: %.2f",
                    ctx.getCurrentKey(), intervalSeconds, lastTempState.value()
            ));

            // 清除状态
            increaseStartTimeState.clear();
            timerState.clear();
        }
    }

    /**
     * 超时告警函数
     * 如果指定时间内没有收到数据，发出告警
     */
    public static class TimeoutAlertFunction
            extends KeyedProcessFunction<String, SensorReading, String> {

        private final int timeoutSeconds;

        // 状态：上一次定时器的时间
        private transient ValueState<Long> lastTimerState;

        public TimeoutAlertFunction(int timeoutSeconds) {
            this.timeoutSeconds = timeoutSeconds;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTimerState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-timer", Long.class)
            );
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out)
                throws Exception {

            // 输出正常数据
            out.collect(String.format(
                    "数据到达：传感器 %s | 温度: %.2f",
                    value.getSensorId(), value.getTemperature()
            ));

            // 删除之前的定时器
            Long lastTimer = lastTimerState.value();
            if (lastTimer != null) {
                ctx.timerService().deleteProcessingTimeTimer(lastTimer);
            }

            // 注册新的定时器
            long currentTime = ctx.timerService().currentProcessingTime();
            long timerTime = currentTime + timeoutSeconds * 1000L;
            ctx.timerService().registerProcessingTimeTimer(timerTime);
            lastTimerState.update(timerTime);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {

            // 定时器触发，说明超时未收到数据
            out.collect(String.format(
                    "超时告警：传感器 %s 已经 %d 秒没有发送数据！",
                    ctx.getCurrentKey(), timeoutSeconds
            ));

            // 清除状态
            lastTimerState.clear();
        }
    }

    /**
     * 温度异常检测器
     * 检测温度突变，并在一段时间后判断是否恢复
     */
    public static class TemperatureAnomalyDetector
            extends KeyedProcessFunction<String, SensorReading, String> {

        private final double threshold;  // 温度突变阈值
        private final int recoverySeconds;  // 恢复判断时间

        // 状态：上一次温度
        private transient ValueState<Double> lastTempState;
        // 状态：异常开始时间
        private transient ValueState<Long> anomalyStartTimeState;
        // 状态：异常时的温度
        private transient ValueState<Double> anomalyTempState;
        // 状态：恢复检查定时器
        private transient ValueState<Long> recoveryTimerState;

        public TemperatureAnomalyDetector(double threshold, int recoverySeconds) {
            this.threshold = threshold;
            this.recoverySeconds = recoverySeconds;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-temp", Double.class)
            );
            anomalyStartTimeState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("anomaly-start-time", Long.class)
            );
            anomalyTempState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("anomaly-temp", Double.class)
            );
            recoveryTimerState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("recovery-timer", Long.class)
            );
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out)
                throws Exception {

            Double lastTemp = lastTempState.value();
            Long anomalyStartTime = anomalyStartTimeState.value();
            Double anomalyTemp = anomalyTempState.value();

            if (lastTemp != null) {
                double tempDiff = value.getTemperature() - lastTemp;

                // 检测温度突变
                if (tempDiff > threshold && anomalyStartTime == null) {
                    // 发现异常
                    long currentTime = ctx.timerService().currentProcessingTime();
                    anomalyStartTimeState.update(currentTime);
                    anomalyTempState.update(value.getTemperature());

                    // 立即发出告警
                    out.collect(String.format(
                            "异常告警：传感器 %s 温度突然升高 %.2f 度！从 %.2f 升至 %.2f",
                            value.getSensorId(), tempDiff, lastTemp, value.getTemperature()
                    ));

                    // 注册恢复检查定时器
                    long timerTime = currentTime + recoverySeconds * 1000L;
                    ctx.timerService().registerProcessingTimeTimer(timerTime);
                    recoveryTimerState.update(timerTime);

                } else if (anomalyStartTime != null) {
                    // 已经在异常状态，检查是否恢复
                    if (Math.abs(value.getTemperature() - anomalyTemp) < threshold / 2) {
                        // 温度恢复正常
                        out.collect(String.format(
                                "恢复通知：传感器 %s 温度已恢复正常，当前温度: %.2f",
                                value.getSensorId(), value.getTemperature()
                        ));

                        // 清除异常状态和定时器
                        Long recoveryTimer = recoveryTimerState.value();
                        if (recoveryTimer != null) {
                            ctx.timerService().deleteProcessingTimeTimer(recoveryTimer);
                        }
                        anomalyStartTimeState.clear();
                        anomalyTempState.clear();
                        recoveryTimerState.clear();
                    }
                }
            }

            // 更新上一次温度
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {

            Double anomalyTemp = anomalyTempState.value();

            if (anomalyTemp != null) {
                // 定时器触发，说明异常状态持续了指定时间
                out.collect(String.format(
                        "持续异常：传感器 %s 异常状态已持续 %d 秒，异常温度: %.2f",
                        ctx.getCurrentKey(), recoverySeconds, anomalyTemp
                ));
            }

            // 清除状态
            anomalyStartTimeState.clear();
            anomalyTempState.clear();
            recoveryTimerState.clear();
        }
    }
}

