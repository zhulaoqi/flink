package com.kinch.flink.sideoutput;

import com.kinch.flink.common.entity.SensorReading;
import com.kinch.flink.common.source.CustomSensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Flink 侧输出流（Side Output）演示
 * <p>
 * 理论知识：
 * 1. 侧输出流概念：
 * - 允许从一个算子输出多个不同类型的数据流
 * - 主流之外的额外输出流
 * - 不同于split/select（已废弃），侧输出流是推荐方案
 * <p>
 * 2. 使用场景：
 * - 数据分流：将不同类型的数据分到不同的流
 * - 异常数据处理：主流处理正常数据，侧输出流处理异常数据
 * - 多级告警：不同级别的告警输出到不同的流
 * - 延迟数据：窗口计算中的迟到数据输出到侧输出流
 * <p>
 * 3. 实现方式：
 * - 使用OutputTag标记不同的侧输出流
 * - 在ProcessFunction中通过Context.output()输出到侧输出流
 * - 通过getSideOutput()获取侧输出流
 * <p>
 * 4. 优势：
 * - 类型安全：每个侧输出流可以有不同的类型
 * - 灵活性高：一个算子可以有多个侧输出流
 * - 性能好：只需要一次处理，避免重复计算
 */
public class SideOutputDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> sensorStream = env.addSource(new CustomSensorSource());

        // ============================================================
        // 1. 基于温度范围的数据分流
        // ============================================================
        // 定义侧输出流标签
        OutputTag<SensorReading> lowTempTag = new OutputTag<SensorReading>("low-temperature") {
        };
        OutputTag<SensorReading> highTempTag = new OutputTag<SensorReading>("high-temperature") {
        };

        // 主流处理正常温度（20-25度），侧输出流处理低温和高温
        SingleOutputStreamOperator<SensorReading> normalTempStream = sensorStream
                .process(new ProcessFunction<SensorReading, SensorReading>() {

                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out)
                            throws Exception {

                        double temp = value.getTemperature();

                        if (temp < 20.0) {
                            // 低温数据输出到侧输出流
                            ctx.output(lowTempTag, value);
                        } else if (temp > 25.0) {
                            // 高温数据输出到侧输出流
                            ctx.output(highTempTag, value);
                        } else {
                            // 正常温度输出到主流
                            out.collect(value);
                        }
                    }
                });

        // 获取侧输出流
        DataStream<SensorReading> lowTempStream = normalTempStream.getSideOutput(lowTempTag);
        DataStream<SensorReading> highTempStream = normalTempStream.getSideOutput(highTempTag);

        // 分别处理不同的流
        normalTempStream.print("正常温度(20-25度)");
        lowTempStream.print("低温告警(<20度)");
        highTempStream.print("高温告警(>25度)");

        // ============================================================
        // 2. 多级告警系统
        // ============================================================
        // 定义不同级别的告警标签
        OutputTag<String> infoAlertTag = new OutputTag<String>("info-alert") {
        };
        OutputTag<String> warningAlertTag = new OutputTag<String>("warning-alert") {
        };
        OutputTag<String> errorAlertTag = new OutputTag<String>("error-alert") {
        };

        SingleOutputStreamOperator<String> criticalAlertStream = sensorStream
                .process(new ProcessFunction<SensorReading, String>() {

                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<String> out)
                            throws Exception {

                        double temp = value.getTemperature();
                        String sensorId = value.getSensorId();

                        if (temp < 18.0) {
                            // 严重告警：温度过低
                            out.collect(String.format(
                                    "严重告警：传感器 %s 温度过低 %.2f 度，可能设备故障！",
                                    sensorId, temp
                            ));
                        } else if (temp < 20.0) {
                            // 错误告警
                            ctx.output(errorAlertTag, String.format(
                                    "错误：传感器 %s 温度 %.2f 度，低于安全范围",
                                    sensorId, temp
                            ));
                        } else if (temp > 27.0) {
                            // 错误告警
                            ctx.output(errorAlertTag, String.format(
                                    "错误：传感器 %s 温度 %.2f 度，高于安全范围",
                                    sensorId, temp
                            ));
                        } else if (temp > 25.0 || temp < 21.0) {
                            // 警告告警
                            ctx.output(warningAlertTag, String.format(
                                    "警告：传感器 %s 温度 %.2f 度，接近安全范围边界",
                                    sensorId, temp
                            ));
                        } else {
                            // 信息告警
                            ctx.output(infoAlertTag, String.format(
                                    "信息：传感器 %s 运行正常，温度 %.2f 度",
                                    sensorId, temp
                            ));
                        }
                    }
                });

        // 获取不同级别的告警流
        DataStream<String> infoAlerts = criticalAlertStream.getSideOutput(infoAlertTag);
        DataStream<String> warningAlerts = criticalAlertStream.getSideOutput(warningAlertTag);
        DataStream<String> errorAlerts = criticalAlertStream.getSideOutput(errorAlertTag);

        // 不同级别的告警可以有不同的处理逻辑
        criticalAlertStream.print("严重告警-需要立即处理");
        errorAlerts.print("错误告警-需要关注");
        warningAlerts.print("警告告警-监控即可");
        infoAlerts.print("信息告警-正常记录");

        // ============================================================
        // 3. 数据质量监控
        // ============================================================
        // 将异常数据分离到侧输出流，主流只保留有效数据
        OutputTag<String> invalidDataTag = new OutputTag<String>("invalid-data") {
        };
        OutputTag<String> duplicateDataTag = new OutputTag<String>("duplicate-data") {
        };

        SingleOutputStreamOperator<SensorReading> validDataStream = sensorStream
                .process(new ProcessFunction<SensorReading, SensorReading>() {

                    private java.util.Set<String> processedIds = new java.util.HashSet<>();

                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out)
                            throws Exception {

                        // 数据有效性检查
                        if (value.getSensorId() == null || value.getSensorId().isEmpty()) {
                            ctx.output(invalidDataTag, "传感器ID为空：" + value.toString());
                            return;
                        }

                        if (value.getTemperature() == null) {
                            ctx.output(invalidDataTag, "温度值为空：" + value.toString());
                            return;
                        }

                        // 温度合理性检查（-50到100度）
                        if (value.getTemperature() < -50.0 || value.getTemperature() > 100.0) {
                            ctx.output(invalidDataTag, String.format(
                                    "温度值异常：传感器 %s 温度 %.2f 度，超出合理范围",
                                    value.getSensorId(), value.getTemperature()
                            ));
                            return;
                        }

                        // 重复数据检查（简化版，实际应该基于时间窗口）
                        String dataKey = value.getSensorId() + "_" + value.getTimestamp();
                        if (processedIds.contains(dataKey)) {
                            ctx.output(duplicateDataTag, "重复数据：" + value.toString());
                            return;
                        }
                        processedIds.add(dataKey);

                        // 保持Set大小，避免内存溢出（实际应该用状态+TTL）
                        if (processedIds.size() > 10000) {
                            processedIds.clear();
                        }

                        // 有效数据输出到主流
                        out.collect(value);
                    }
                });

        // 获取异常数据流
        DataStream<String> invalidData = validDataStream.getSideOutput(invalidDataTag);
        DataStream<String> duplicateData = validDataStream.getSideOutput(duplicateDataTag);

        // 不同类型的异常数据可以有不同的处理策略
        validDataStream.print("有效数据");
        invalidData.print("无效数据-需要修复");
        duplicateData.print("重复数据-需要去重");

        // ============================================================
        // 4. 实时ETL：数据清洗和转换
        // ============================================================
        // 定义不同类型数据的输出标签
        OutputTag<String> sensor1DataTag = new OutputTag<String>("sensor-1-data") {
        };
        OutputTag<String> sensor2DataTag = new OutputTag<String>("sensor-2-data") {
        };
        OutputTag<String> otherSensorDataTag = new OutputTag<String>("other-sensor-data") {
        };

        SingleOutputStreamOperator<String> etlMainStream = sensorStream
                .process(new ProcessFunction<SensorReading, String>() {

                    @Override
                    public void processElement(SensorReading value, Context ctx, Collector<String> out)
                            throws Exception {

                        // 根据传感器ID将数据路由到不同的流
                        String sensorId = value.getSensorId();
                        String data = String.format(
                                "传感器: %s, 温度: %.2f, 时间: %d",
                                sensorId, value.getTemperature(), value.getTimestamp()
                        );

                        switch (sensorId) {
                            case "sensor_1":
                                ctx.output(sensor1DataTag, data);
                                break;
                            case "sensor_2":
                                ctx.output(sensor2DataTag, data);
                                break;
                            default:
                                if (sensorId.startsWith("sensor_")) {
                                    ctx.output(otherSensorDataTag, data);
                                } else {
                                    // 未识别的传感器输出到主流
                                    out.collect("未知传感器：" + data);
                                }
                                break;
                        }
                    }
                });

        // 获取不同传感器的数据流
        DataStream<String> sensor1Data = etlMainStream.getSideOutput(sensor1DataTag);
        DataStream<String> sensor2Data = etlMainStream.getSideOutput(sensor2DataTag);
        DataStream<String> otherSensorData = etlMainStream.getSideOutput(otherSensorDataTag);

        // 每个传感器的数据可以独立处理和存储
        sensor1Data.print("传感器1专用流");
        sensor2Data.print("传感器2专用流");
        otherSensorData.print("其他传感器流");
        etlMainStream.print("未知传感器流");

        env.execute("Flink Side Output Demo");

        /*
         * 侧输出流总结：
         *
         * 1. 定义OutputTag：
         *    - 使用匿名内部类：new OutputTag<T>("tag-name"){}
         *    - 泛型指定侧输出流的数据类型
         *    - tag名称用于标识不同的侧输出流
         *
         * 2. 输出数据：
         *    - 在ProcessFunction的processElement方法中
         *    - 使用ctx.output(outputTag, data)输出到侧输出流
         *    - 使用out.collect(data)输出到主流
         *
         * 3. 获取侧输出流：
         *    - 调用getSideOutput(outputTag)获取侧输出流
         *    - 返回的是DataStream<T>，可以继续进行转换操作
         *
         * 4. 与split/select对比：
         *    - split/select已废弃，不支持不同类型
         *    - 侧输出流支持不同类型，更灵活
         *    - 侧输出流只需要处理一次，性能更好
         *
         * 5. 使用场景选择：
         *    - 数据分流 → 侧输出流
         *    - 异常处理 → 侧输出流
         *    - 多级告警 → 侧输出流
         *    - 延迟数据 → allowedLateness + sideOutputLateData
         *
         * 6. 最佳实践：
         *    - 为每个侧输出流定义明确的语义
         *    - 使用有意义的tag名称
         *    - 主流和侧输出流的处理逻辑要清晰
         *    - 避免过多的侧输出流，影响可维护性
         *
         * 7. 性能考虑：
         *    - 侧输出流不会增加额外的序列化开销
         *    - 数据只处理一次，比多次filter高效
         *    - 适合复杂的数据分发场景
         */
    }
}

