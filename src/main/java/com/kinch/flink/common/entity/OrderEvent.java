package com.kinch.flink.common.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 订单事件实体类
 * 用于模拟电商订单事件数据
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 订单ID
     */
    private String orderId;

    /**
     * 用户ID
     */
    private Long userId;

    /**
     * 事件类型: create(创建订单), pay(支付), cancel(取消)
     */
    private String eventType;

    /**
     * 订单金额
     */
    private Double amount;

    /**
     * 事件时间戳（毫秒）
     */
    private Long timestamp;
}

