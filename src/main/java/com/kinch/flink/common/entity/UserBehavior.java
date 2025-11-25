package com.kinch.flink.common.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 用户行为实体类
 * 用于模拟电商用户行为数据
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 用户ID
     */
    private Long userId;

    /**
     * 商品ID
     */
    private Long itemId;

    /**
     * 商品类目ID
     */
    private Integer categoryId;

    /**
     * 行为类型: pv(浏览), cart(加购物车), fav(收藏), buy(购买)
     */
    private String behavior;

    /**
     * 行为时间戳（毫秒）
     */
    private Long timestamp;
}

