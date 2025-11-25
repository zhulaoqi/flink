package com.kinch.flink.common.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 传感器读数实体类
 * 用于模拟物联网传感器数据
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 传感器ID
     */
    private String sensorId;

    /**
     * 时间戳（毫秒）
     */
    private Long timestamp;

    /**
     * 温度值
     */
    private Double temperature;
}

