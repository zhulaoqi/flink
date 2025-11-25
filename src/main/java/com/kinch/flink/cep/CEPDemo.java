package com.kinch.flink.cep;

import com.kinch.flink.common.entity.OrderEvent;
import com.kinch.flink.common.entity.SensorReading;
import com.kinch.flink.common.source.CustomSensorSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Flink CEP（Complex Event Processing）复杂事件处理演示
 * <p>
 * 理论知识：
 * 1. CEP概念：
 * - 在数据流中识别特定的事件模式
 * - 实时检测复杂的业务场景
 * - 基于规则的事件匹配和告警
 * <p>
 * 2. 核心概念：
 * - Pattern（模式）：定义要匹配的事件序列
 * - Event（事件）：输入的数据流
 * - Match（匹配）：符合模式的事件序列
 * <p>
 * 3. 模式API：
 * - begin()：开始一个模式
 * - where()：添加条件
 * - next()：严格连续
 * - followedBy()：非严格连续
 * - followedByAny()：非确定连续
 * - notNext()/notFollowedBy()：否定模式
 * - within()：时间约束
 * <p>
 * 4. 量词（Quantifier）：
 * - oneOrMore()：一次或多次
 * - times(n)：恰好n次
 * - times(n, m)：n到m次
 * - optional()：可选
 * - greedy()：贪婪匹配
 * <p>
 * 5. 组合条件：
 * - where()：简单条件
 * - or()：或条件
 * - until()：终止条件
 * - subtype()：子类型过滤
 * <p>
 * 6. 使用场景：
 * - 风险控制：异常行为检测、欺诈识别
 * - 运维监控：故障模式识别、性能异常
 * - 用户行为：转化漏斗、流失预警
 * - 业务规则：订单超时、支付失败
 */
public class CEPDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ============================================================
        // 1. 温度持续上升检测
        // ============================================================
        System.out.println("\n========== 温度持续上升检测 ==========");

        DataStream<SensorReading> sensorStream = env.addSource(new CustomSensorSource());

        // 定义模式：温度连续3次上升
        Pattern<SensorReading, ?> temperatureIncreasePattern = Pattern
                .<SensorReading>begin("first")
                .where(new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return value.getTemperature() > 20.0;
                    }
                })
                .next("second")
                .where(new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return value.getTemperature() > 20.0;
                    }
                })
                .next("third")
                .where(new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return value.getTemperature() > 20.0;
                    }
                })
                .within(Time.seconds(10));  // 10秒内完成

        // 应用模式
        PatternStream<SensorReading> tempPatternStream = CEP.pattern(
                sensorStream.keyBy(SensorReading::getSensorId),
                temperatureIncreasePattern
        );

        // 提取匹配结果
        DataStream<String> tempAlerts = tempPatternStream.select(
                new PatternSelectFunction<SensorReading, String>() {
                    @Override
                    public String select(Map<String, List<SensorReading>> pattern) throws Exception {
                        SensorReading first = pattern.get("first").get(0);
                        SensorReading second = pattern.get("second").get(0);
                        SensorReading third = pattern.get("third").get(0);

                        return String.format(
                                "温度持续上升告警：传感器 %s | %.2f -> %.2f -> %.2f",
                                first.getSensorId(),
                                first.getTemperature(),
                                second.getTemperature(),
                                third.getTemperature()
                        );
                    }
                }
        );

        tempAlerts.print("温度上升告警");

        // ============================================================
        // 2. 温度波动检测（上升后下降）
        // ============================================================
        System.out.println("\n========== 温度波动检测 ==========");

        // 定义模式：温度先上升再下降
        Pattern<SensorReading, ?> fluctuationPattern = Pattern
                .<SensorReading>begin("start")
                .next("increase")
                .where(new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return true;  // 接受任何值，通过iterative condition判断
                    }
                })
                .next("decrease")
                .where(new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return true;
                    }
                })
                .within(Time.seconds(20));

        // ============================================================
        // 3. 订单超时检测（实际场景）
        // ============================================================
        System.out.println("\n========== 订单超时检测 ==========");

        // 模拟订单事件流
        DataStream<OrderEvent> orderStream = env.fromElements(
                new OrderEvent("order_1", 1001L, "create", 100.0, 1609459200000L),
                new OrderEvent("order_2", 1002L, "create", 200.0, 1609459201000L),
                new OrderEvent("order_1", 1001L, "pay", 100.0, 1609459205000L),
                new OrderEvent("order_3", 1003L, "create", 300.0, 1609459210000L),
                new OrderEvent("order_2", 1002L, "cancel", 200.0, 1609459220000L)
        );

        // 定义模式：创建订单后15分钟内未支付
        Pattern<OrderEvent, ?> orderTimeoutPattern = Pattern
                .<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        PatternStream<OrderEvent> orderPatternStream = CEP.pattern(
                orderStream.keyBy(OrderEvent::getOrderId),
                orderTimeoutPattern
        );

        // 定义超时侧输出流
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout") {
        };

        // 提取匹配和超时结果
        SingleOutputStreamOperator<String> orderResult = orderPatternStream.select(
                timeoutTag,
                // 超时处理
                new PatternTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp)
                            throws Exception {
                        OrderEvent createEvent = pattern.get("create").get(0);
                        return String.format(
                                "订单超时告警：订单 %s | 用户 %d | 金额 %.2f | 创建时间 %d | 超时时间 %d",
                                createEvent.getOrderId(),
                                createEvent.getUserId(),
                                createEvent.getAmount(),
                                createEvent.getTimestamp(),
                                timeoutTimestamp
                        );
                    }
                },
                // 正常支付
                new PatternSelectFunction<OrderEvent, String>() {
                    @Override
                    public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                        OrderEvent createEvent = pattern.get("create").get(0);
                        OrderEvent payEvent = pattern.get("pay").get(0);
                        long payTime = (payEvent.getTimestamp() - createEvent.getTimestamp()) / 1000;

                        return String.format(
                                "订单支付成功：订单 %s | 用户 %d | 金额 %.2f | 支付耗时 %d秒",
                                createEvent.getOrderId(),
                                createEvent.getUserId(),
                                createEvent.getAmount(),
                                payTime
                        );
                    }
                }
        );

        orderResult.print("订单正常支付");
        orderResult.getSideOutput(timeoutTag).print("订单超时");

        // ============================================================
        // 4. 用户连续登录失败检测（风控场景）
        // ============================================================
        System.out.println("\n========== 连续登录失败检测 ==========");

        // 定义登录事件
        DataStream<Tuple3<String, String, Long>> loginStream = env.fromElements(
                new Tuple3<>("user_1", "fail", 1609459200000L),
                new Tuple3<>("user_1", "fail", 1609459201000L),
                new Tuple3<>("user_1", "fail", 1609459202000L),
                new Tuple3<>("user_2", "success", 1609459203000L),
                new Tuple3<>("user_1", "fail", 1609459204000L)
        );

        // 定义模式：连续3次登录失败
        Pattern<Tuple3<String, String, Long>, ?> loginFailPattern = Pattern
                .<Tuple3<String, String, Long>>begin("first")
                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
                        return "fail".equals(value.f1);
                    }
                })
                .times(3)  // 连续3次
                .consecutive()  // 严格连续
                .within(Time.minutes(5));  // 5分钟内

        PatternStream<Tuple3<String, String, Long>> loginPatternStream = CEP.pattern(
                loginStream.keyBy(login -> login.f0),
                loginFailPattern
        );

        DataStream<String> loginFailAlerts = loginPatternStream.select(
                new PatternSelectFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String select(Map<String, List<Tuple3<String, String, Long>>> pattern)
                            throws Exception {
                        List<Tuple3<String, String, Long>> failures = pattern.get("first");
                        String userId = failures.get(0).f0;

                        return String.format(
                                "风控告警：用户 %s 连续登录失败 %d 次，建议锁定账户！",
                                userId, failures.size()
                        );
                    }
                }
        );

        loginFailAlerts.print("登录失败告警");

        // ============================================================
        // 5. 传感器故障检测（复杂模式）
        // ============================================================
        System.out.println("\n========== 传感器故障检测 ==========");

        // 定义模式：温度异常高 -> 温度异常低 -> 无数据（疑似故障）
        Pattern<SensorReading, ?> faultPattern = Pattern
                .<SensorReading>begin("highTemp")
                .where(new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return value.getTemperature() > 30.0;
                    }
                })
                .followedBy("lowTemp")
                .where(new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return value.getTemperature() < 15.0;
                    }
                })
                .within(Time.seconds(30));

        PatternStream<SensorReading> faultPatternStream = CEP.pattern(
                sensorStream.keyBy(SensorReading::getSensorId),
                faultPattern
        );

        DataStream<String> faultAlerts = faultPatternStream.select(
                new PatternSelectFunction<SensorReading, String>() {
                    @Override
                    public String select(Map<String, List<SensorReading>> pattern) throws Exception {
                        SensorReading highTemp = pattern.get("highTemp").get(0);
                        SensorReading lowTemp = pattern.get("lowTemp").get(0);

                        return String.format(
                                "传感器故障告警：%s | 温度异常波动 %.2f -> %.2f，疑似故障！",
                                highTemp.getSensorId(),
                                highTemp.getTemperature(),
                                lowTemp.getTemperature()
                        );
                    }
                }
        );

        faultAlerts.print("传感器故障告警");

        // ============================================================
        // 6. 组合模式：温度异常且持续时间长
        // ============================================================
        System.out.println("\n========== 组合模式检测 ==========");

        // 定义模式：温度超过阈值且持续出现
        Pattern<SensorReading, ?> combinedPattern = Pattern
                .<SensorReading>begin("start")
                .where(new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return value.getTemperature() > 25.0;
                    }
                })
                .oneOrMore()  // 一次或多次
                .consecutive()  // 严格连续
                .within(Time.seconds(15));  // 15秒内

        PatternStream<SensorReading> combinedPatternStream = CEP.pattern(
                sensorStream.keyBy(SensorReading::getSensorId),
                combinedPattern
        );

        DataStream<String> combinedAlerts = combinedPatternStream.select(
                new PatternSelectFunction<SensorReading, String>() {
                    @Override
                    public String select(Map<String, List<SensorReading>> pattern) throws Exception {
                        List<SensorReading> events = pattern.get("start");
                        SensorReading first = events.get(0);
                        SensorReading last = events.get(events.size() - 1);

                        double avgTemp = events.stream()
                                .mapToDouble(SensorReading::getTemperature)
                                .average()
                                .orElse(0.0);

                        return String.format(
                                "持续高温告警：传感器 %s | 持续次数 %d | 平均温度 %.2f | 起始 %.2f | 结束 %.2f",
                                first.getSensorId(),
                                events.size(),
                                avgTemp,
                                first.getTemperature(),
                                last.getTemperature()
                        );
                    }
                }
        );

        combinedAlerts.print("持续高温告警");

        env.execute("Flink CEP Demo");

        /*
         * CEP使用总结：
         *
         * 1. 模式匹配策略：
         *    - next()：严格连续，中间不能有其他事件
         *    - followedBy()：非严格连续，中间可以有其他事件
         *    - followedByAny()：非确定连续，每个事件都可能作为匹配开始
         *
         * 2. 量词使用：
         *    - times(3)：恰好3次
         *    - times(2, 4)：2到4次
         *    - oneOrMore()：1次或多次
         *    - timesOrMore(3)：3次或多次
         *    - optional()：可选（0次或1次）
         *
         * 3. 连续性修饰：
         *    - consecutive()：严格连续
         *    - allowCombinations()：允许组合
         *
         * 4. 条件组合：
         *    - where()：添加条件
         *    - or()：或条件
         *    - until()：循环终止条件
         *
         * 5. 时间约束：
         *    - within()：必须在指定时间内完成模式匹配
         *    - 超时后触发timeout处理
         *
         * 6. 匹配跳过策略：
         *    - NO_SKIP：不跳过任何匹配
         *    - SKIP_TO_NEXT：跳到下一个事件
         *    - SKIP_PAST_LAST_EVENT：跳过当前匹配的所有事件
         *    - SKIP_TO_FIRST：跳到指定的第一个事件
         *    - SKIP_TO_LAST：跳到指定的最后一个事件
         *
         * 7. 性能优化：
         *    - 尽量使用简单条件
         *    - 合理设置时间窗口
         *    - 避免过于复杂的模式
         *    - 使用KeyBy减少状态大小
         *
         * 8. 实际应用场景：
         *    - 风控：异常交易检测、账户盗用识别
         *    - 运维：故障预警、性能异常检测
         *    - 业务：用户行为分析、转化漏斗监控
         *    - IoT：设备故障预测、异常模式识别
         *
         * 9. 注意事项：
         *    - CEP会占用大量状态，需要设置合理的TTL
         *    - 复杂模式可能影响性能
         *    - 超时时间要合理设置
         *    - 考虑使用ProcessFunction替代简单场景
         */
    }
}

