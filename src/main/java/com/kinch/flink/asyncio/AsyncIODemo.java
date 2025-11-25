package com.kinch.flink.asyncio;

import com.kinch.flink.common.entity.SensorReading;
import com.kinch.flink.common.source.CustomSensorSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.*;

/**
 * Flink 异步IO（Async I/O）演示
 * <p>
 * 理论知识：
 * 1. 异步IO概念：
 * - 允许在流处理过程中进行异步的外部数据访问
 * - 不会阻塞整个算子，提高吞吐量
 * - 特别适合访问外部数据库、缓存、HTTP服务等
 * <p>
 * 2. 为什么需要异步IO：
 * - 传统同步IO会阻塞线程，等待外部系统响应
 * - 异步IO可以在等待期间处理其他数据
 * - 大幅提升吞吐量（特别是外部调用延迟高时）
 * <p>
 * 3. 工作原理：
 * - 发起异步请求后立即返回，不阻塞
 * - 使用回调函数处理异步结果
 * - Flink维护一个队列管理未完成的异步请求
 * <p>
 * 4. 核心组件：
 * - RichAsyncFunction：异步函数基类
 * - ResultFuture：用于返回异步结果
 * - AsyncDataStream：异步IO的工具类
 * <p>
 * 5. 顺序保证：
 * - unorderedWait：无序，先完成先输出（吞吐量高）
 * - orderedWait：有序，按输入顺序输出（保序性）
 * <p>
 * 6. 超时处理：
 * - 设置超时时间，避免无限等待
 * - 超时后可以返回默认值或抛出异常
 * <p>
 * 7. 容量限制：
 * - 限制同时进行的异步请求数量
 * - 避免外部系统过载
 */
public class AsyncIODemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> sensorStream = env.addSource(new CustomSensorSource());

        // ============================================================
        // 1. 异步查询传感器元数据（无序）
        // ============================================================
        // 无序模式：先完成的请求先输出，吞吐量最高
        DataStream<Tuple2<SensorReading, String>> enrichedDataUnordered = AsyncDataStream
                .unorderedWait(
                        sensorStream,
                        new AsyncDatabaseRequest(),  // 异步函数
                        5000,                         // 超时时间（毫秒）
                        TimeUnit.MILLISECONDS,
                        100                           // 最大并发请求数
                );

        enrichedDataUnordered.print("异步查询-无序输出");

        // ============================================================
        // 2. 异步查询传感器元数据（有序）
        // ============================================================
        // 有序模式：按输入顺序输出，保证顺序性
        DataStream<Tuple2<SensorReading, String>> enrichedDataOrdered = AsyncDataStream
                .orderedWait(
                        sensorStream,
                        new AsyncDatabaseRequest(),
                        5000,
                        TimeUnit.MILLISECONDS,
                        100
                );

        enrichedDataOrdered.print("异步查询-有序输出");

        // ============================================================
        // 3. 异步HTTP请求示例
        // ============================================================
        DataStream<String> httpResultStream = AsyncDataStream
                .unorderedWait(
                        sensorStream,
                        new AsyncHttpRequest(),
                        3000,
                        TimeUnit.MILLISECONDS,
                        50
                );

        httpResultStream.print("异步HTTP请求");

        // ============================================================
        // 4. 异步缓存查询示例
        // ============================================================
        DataStream<String> cacheResultStream = AsyncDataStream
                .unorderedWait(
                        sensorStream,
                        new AsyncCacheRequest(),
                        1000,
                        TimeUnit.MILLISECONDS,
                        200
                );

        cacheResultStream.print("异步缓存查询");

        env.execute("Flink Async I/O Demo");

        /*
         * 异步IO性能对比：
         *
         * 场景：访问外部数据库，每次请求延迟10ms
         *
         * 1. 同步IO：
         *    - 吞吐量：100 QPS（1000ms / 10ms）
         *    - 资源利用率：低（大量时间在等待）
         *
         * 2. 异步IO（并发100）：
         *    - 吞吐量：10000 QPS（100 * 100 QPS）
         *    - 资源利用率：高（等待期间处理其他请求）
         *
         * 性能提升：100倍
         *
         * 使用建议：
         *
         * 1. 适用场景：
         *    - 需要查询外部系统（数据库、缓存、HTTP API）
         *    - 外部调用延迟较高（>5ms）
         *    - 需要高吞吐量
         *
         * 2. 不适用场景：
         *    - 外部调用延迟极低（<1ms）
         *    - 需要严格的顺序保证（使用orderedWait会降低性能）
         *    - 外部系统不支持异步调用
         *
         * 3. 配置建议：
         *    - 超时时间：根据外部系统响应时间设置（P99延迟 * 2）
         *    - 并发数：根据外部系统容量设置（不要打爆外部系统）
         *    - 顺序模式：优先使用unorderedWait，除非必须保序
         *
         * 4. 异常处理：
         *    - 超时：返回默认值或重试
         *    - 外部系统异常：重试或降级
         *    - 记录失败请求，用于排查问题
         *
         * 5. 监控指标：
         *    - 异步请求延迟（P50, P95, P99）
         *    - 超时率
         *    - 外部系统错误率
         *    - 并发请求数
         */
    }

    /**
     * 模拟异步数据库查询
     */
    public static class AsyncDatabaseRequest
            extends RichAsyncFunction<SensorReading, Tuple2<SensorReading, String>> {

        // 模拟数据库连接池
        private transient ExecutorService executorService;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化线程池用于模拟异步操作
            executorService = Executors.newFixedThreadPool(10);
        }

        @Override
        public void close() throws Exception {
            // 关闭线程池
            if (executorService != null) {
                executorService.shutdown();
            }
        }

        @Override
        public void asyncInvoke(SensorReading input, ResultFuture<Tuple2<SensorReading, String>> resultFuture)
                throws Exception {

            // 提交异步任务
            CompletableFuture.supplyAsync(() -> {
                try {
                    // 模拟数据库查询延迟（50-150ms）
                    Thread.sleep(50 + (long) (Math.random() * 100));

                    // 模拟查询传感器元数据
                    String metadata = queryDatabase(input.getSensorId());

                    return new Tuple2<>(input, metadata);

                } catch (Exception e) {
                    // 异常处理
                    return new Tuple2<>(input, "查询失败: " + e.getMessage());
                }
            }, executorService).thenAccept((Tuple2<SensorReading, String> result) -> {
                // 异步操作完成，返回结果
                resultFuture.complete(Collections.singleton(result));
            });
        }

        /**
         * 模拟数据库查询
         */
        private String queryDatabase(String sensorId) {
            // 实际场景：
            // 1. 使用支持异步的数据库驱动（如Vert.x MySQL Client）
            // 2. 使用Redis异步客户端（如Lettuce）
            // 3. 使用HTTP异步客户端（如AsyncHttpClient）

            return String.format(
                    "传感器元数据 [位置: 机房A-%s区, 型号: TMP-100, 状态: 正常]",
                    sensorId.substring(sensorId.length() - 1)
            );
        }

        @Override
        public void timeout(SensorReading input, ResultFuture<Tuple2<SensorReading, String>> resultFuture)
                throws Exception {
            // 超时处理：返回默认值
            System.out.println("查询超时：" + input.getSensorId());
            resultFuture.complete(Collections.singleton(
                    new Tuple2<>(input, "查询超时-使用默认值")
            ));
        }
    }

    /**
     * 模拟异步HTTP请求
     */
    public static class AsyncHttpRequest extends RichAsyncFunction<SensorReading, String> {

        private transient ExecutorService executorService;

        @Override
        public void open(Configuration parameters) throws Exception {
            executorService = Executors.newFixedThreadPool(5);
        }

        @Override
        public void close() throws Exception {
            if (executorService != null) {
                executorService.shutdown();
            }
        }

        @Override
        public void asyncInvoke(SensorReading input, ResultFuture<String> resultFuture)
                throws Exception {

            CompletableFuture.supplyAsync(() -> {
                try {
                    // 模拟HTTP请求延迟
                    Thread.sleep(30 + (long) (Math.random() * 70));

                    // 模拟调用外部天气API获取温度校准系数
                    double calibrationFactor = callWeatherAPI(input.getTemperature());
                    double calibratedTemp = input.getTemperature() * calibrationFactor;

                    return String.format(
                            "传感器: %s | 原始温度: %.2f | 校准系数: %.2f | 校准后温度: %.2f",
                            input.getSensorId(), input.getTemperature(),
                            calibrationFactor, calibratedTemp
                    );

                } catch (Exception e) {
                    return "HTTP请求失败: " + e.getMessage();
                }
            }, executorService).thenAccept((String result) -> {
                resultFuture.complete(Collections.singleton(result));
            });
        }

        /**
         * 模拟调用外部API
         */
        private double callWeatherAPI(double temperature) {
            // 实际场景：
            // 使用AsyncHttpClient、Netty等异步HTTP客户端
            // HttpAsyncClient client = HttpAsyncClients.createDefault();
            // client.execute(request, callback);

            // 根据温度返回校准系数（模拟）
            if (temperature < 20) {
                return 1.02;  // 低温传感器偏差+2%
            } else if (temperature > 25) {
                return 0.98;  // 高温传感器偏差-2%
            } else {
                return 1.0;   // 正常温度无偏差
            }
        }

        @Override
        public void timeout(SensorReading input, ResultFuture<String> resultFuture)
                throws Exception {
            resultFuture.complete(Collections.singleton(
                    "HTTP请求超时：" + input.getSensorId()
            ));
        }
    }

    /**
     * 模拟异步缓存查询（Redis）
     */
    public static class AsyncCacheRequest extends RichAsyncFunction<SensorReading, String> {

        private transient ExecutorService executorService;
        private transient ConcurrentHashMap<String, String> mockCache;

        @Override
        public void open(Configuration parameters) throws Exception {
            executorService = Executors.newFixedThreadPool(10);

            // 模拟缓存初始化
            mockCache = new ConcurrentHashMap<>();
            mockCache.put("sensor_1", "一号车间主传感器");
            mockCache.put("sensor_2", "二号车间主传感器");
            mockCache.put("sensor_3", "三号车间主传感器");
            mockCache.put("sensor_4", "四号车间主传感器");
            mockCache.put("sensor_5", "五号车间主传感器");
        }

        @Override
        public void close() throws Exception {
            if (executorService != null) {
                executorService.shutdown();
            }
        }

        @Override
        public void asyncInvoke(SensorReading input, ResultFuture<String> resultFuture)
                throws Exception {

            CompletableFuture.supplyAsync(() -> {
                try {
                    // 模拟Redis查询延迟（1-5ms）
                    Thread.sleep(1 + (long) (Math.random() * 4));

                    // 查询缓存
                    String sensorName = mockCache.getOrDefault(
                            input.getSensorId(),
                            "未知传感器"
                    );

                    return String.format(
                            "[缓存查询] 传感器: %s (%s) | 温度: %.2f 度",
                            input.getSensorId(), sensorName, input.getTemperature()
                    );

                } catch (Exception e) {
                    return "缓存查询失败: " + e.getMessage();
                }
            }, executorService).thenAccept((String result) -> {
                resultFuture.complete(Collections.singleton(result));
            });
        }

        @Override
        public void timeout(SensorReading input, ResultFuture<String> resultFuture)
                throws Exception {
            // 缓存查询超时，使用默认值
            resultFuture.complete(Collections.singleton(
                    "缓存查询超时：" + input.getSensorId() + " 使用默认值"
            ));
        }
    }
}

