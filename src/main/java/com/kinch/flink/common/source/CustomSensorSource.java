package com.kinch.flink.common.source;

import com.kinch.flink.common.entity.SensorReading;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 自定义传感器数据源
 * 模拟生成传感器温度数据
 */
public class CustomSensorSource implements SourceFunction<SensorReading> {

    private volatile boolean running = true;
    private final Random random = new Random();

    // 传感器ID列表
    private final String[] sensorIds = {"sensor_1", "sensor_2", "sensor_3", "sensor_4", "sensor_5"};

    // 初始温度
    private final double[] currentTemperatures = {20.0, 21.0, 22.0, 23.0, 24.0};

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        while (running) {
            for (int i = 0; i < sensorIds.length; i++) {
                // 温度随机波动 -0.5 到 +0.5 度
                currentTemperatures[i] += (random.nextDouble() - 0.5);

                SensorReading reading = new SensorReading(
                        sensorIds[i],
                        System.currentTimeMillis(),
                        currentTemperatures[i]
                );

                ctx.collect(reading);
            }

            // 每秒生成一次数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}

