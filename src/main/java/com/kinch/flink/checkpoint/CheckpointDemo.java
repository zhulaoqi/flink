package com.kinch.flink.checkpoint;

import com.kinch.flink.common.entity.SensorReading;
import com.kinch.flink.common.source.CustomSensorSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink Checkpoint和容错机制演示
 * <p>
 * 理论知识：
 * 1. Checkpoint概念：
 * - Checkpoint是Flink容错机制的核心
 * - 定期对所有算子的状态进行快照
 * - 故障恢复时从最近的Checkpoint恢复
 * <p>
 * 2. Checkpoint工作原理（Chandy-Lamport算法）：
 * a. JobManager触发Checkpoint
 * b. Source注入Barrier（分界线）到数据流
 * c. Barrier随数据流向下游传播
 * d. 算子收到Barrier后保存状态快照
 * e. 所有算子完成快照后，Checkpoint完成
 * <p>
 * 3. Checkpoint配置：
 * - 间隔时间：多久触发一次
 * - 模式：EXACTLY_ONCE（精确一次）或 AT_LEAST_ONCE（至少一次）
 * - 超时时间：Checkpoint最长执行时间
 * - 最小间隔：两次Checkpoint之间的最小时间
 * - 最大并发：同时进行的Checkpoint数量
 * <p>
 * 4. StateBackend（状态后端）：
 * - HashMapStateBackend：内存存储，快照到文件系统（默认）
 * - EmbeddedRocksDBStateBackend：RocksDB存储，适合大状态
 * <p>
 * 5. Checkpoint存储：
 * - JobManagerCheckpointStorage：存储在JobManager内存（仅测试）
 * - FileSystemCheckpointStorage：存储在文件系统（生产推荐）
 * <p>
 * 6. 重启策略：
 * - 固定延迟：尝试固定次数，每次间隔固定时间
 * - 失败率：在时间窗口内失败次数不超过阈值
 * - 无重启：不重启（仅测试）
 * <p>
 * 7. Savepoint vs Checkpoint：
 * - Checkpoint：自动、轻量、临时、用于故障恢复
 * - Savepoint：手动、完整、永久、用于版本升级和运维
 */
public class CheckpointDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // ============================================================
        // 1. 启用Checkpoint
        // ============================================================
        // 每5秒触发一次Checkpoint
        env.enableCheckpointing(5000);

        // ============================================================
        // 2. Checkpoint配置
        // ============================================================
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // Checkpoint模式：EXACTLY_ONCE保证精确一次语义
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // Checkpoint超时时间：60秒
        // 如果Checkpoint在60秒内未完成，会被取消
        checkpointConfig.setCheckpointTimeout(60000);

        // 两次Checkpoint之间的最小时间间隔：1秒
        // 确保上一次Checkpoint完成后，至少等待1秒再开始下一次
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);

        // 最大并发Checkpoint数量：1
        // 同一时间只允许一个Checkpoint进行
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // Checkpoint取消后是否保留：在任务取消时保留Checkpoint
        // RETAIN_ON_CANCELLATION：保留
        // DELETE_ON_CANCELLATION：删除（默认）
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // 允许的连续Checkpoint失败次数：3次
        // 超过3次后任务失败
        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        // ============================================================
        // 3. 配置StateBackend（状态后端）
        // ============================================================
        // 方式1：HashMapStateBackend（适合中等状态）
        // 状态存储在TaskManager的JVM堆内存中
        // Checkpoint时将状态快照到文件系统
        HashMapStateBackend hashMapStateBackend = new HashMapStateBackend();
        env.setStateBackend(hashMapStateBackend);

        // 方式2：EmbeddedRocksDBStateBackend（适合大状态）
        // 状态存储在RocksDB（本地磁盘）
        // Checkpoint时增量快照
        // EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend();
        // rocksDBStateBackend.setDbStoragePath("/tmp/flink/rocksdb");  // RocksDB本地存储路径
        // env.setStateBackend(rocksDBStateBackend);

        // ============================================================
        // 4. 配置Checkpoint存储位置
        // ============================================================
        // 设置Checkpoint存储路径（生产环境应使用HDFS或对象存储）
        // 格式：file:/// 或 hdfs:// 或 s3://
        checkpointConfig.setCheckpointStorage(
                new FileSystemCheckpointStorage("file:///tmp/flink/checkpoints")
        );

        // ============================================================
        // 5. 配置重启策略
        // ============================================================
        // 方式1：固定延迟重启策略
        // 尝试重启3次，每次间隔10秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,      // 重启次数
                Time.seconds(10)  // 重启间隔
        ));

        // 方式2：失败率重启策略
        // 在5分钟内最多失败3次，每次重启间隔10秒
        // env.setRestartStrategy(RestartStrategies.failureRateRestart(
        //     3,                      // 最大失败次数
        //     Time.minutes(5),        // 统计时间窗口
        //     Time.seconds(10)        // 重启间隔
        // ));

        // 方式3：无重启策略（仅测试）
        // env.setRestartStrategy(RestartStrategies.noRestart());

        // ============================================================
        // 6. 创建数据流并进行处理
        // ============================================================
        DataStream<SensorReading> sensorStream = env.addSource(new CustomSensorSource());

        // 带状态的处理
        DataStream<String> resultStream = sensorStream
                .keyBy(sensor -> sensor.getSensorId())
                .map(new org.apache.flink.api.common.functions.RichMapFunction<SensorReading, String>() {

                    private transient org.apache.flink.api.common.state.ValueState<Double> maxTempState;
                    private transient org.apache.flink.api.common.state.ValueState<Long> countState;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                        // 最高温度状态
                        maxTempState = getRuntimeContext().getState(
                                new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                                        "max-temp", Double.class
                                )
                        );

                        // 数据计数状态
                        countState = getRuntimeContext().getState(
                                new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                                        "count", Long.class
                                )
                        );
                    }

                    @Override
                    public String map(SensorReading value) throws Exception {
                        // 获取当前状态
                        Double maxTemp = maxTempState.value();
                        Long count = countState.value();

                        // 初始化
                        if (maxTemp == null) {
                            maxTemp = Double.MIN_VALUE;
                        }
                        if (count == null) {
                            count = 0L;
                        }

                        // 更新状态
                        if (value.getTemperature() > maxTemp) {
                            maxTemp = value.getTemperature();
                        }
                        count++;

                        maxTempState.update(maxTemp);
                        countState.update(count);

                        // 这些状态会被周期性地Checkpoint
                        return String.format(
                                "[Checkpoint保护] 传感器: %s | 当前温度: %.2f | 历史最高: %.2f | 数据条数: %d",
                                value.getSensorId(), value.getTemperature(), maxTemp, count
                        );
                    }
                });

        resultStream.print("状态统计");

        // ============================================================
        // 7. 模拟故障恢复
        // ============================================================
        // 添加一个会偶尔抛出异常的算子，模拟故障
        DataStream<String> faultTolerantStream = resultStream
                .map(new org.apache.flink.api.common.functions.RichMapFunction<String, String>() {

                    private int processedCount = 0;

                    @Override
                    public String map(String value) throws Exception {
                        processedCount++;

                        // 每处理100条数据模拟一次故障
                        // Flink会从最近的Checkpoint恢复
                        // if (processedCount % 100 == 0) {
                        //     throw new RuntimeException("模拟故障：触发Checkpoint恢复");
                        // }

                        return value + " [已处理: " + processedCount + "条]";
                    }
                });

        faultTolerantStream.print("容错处理");

        env.execute("Flink Checkpoint Demo");

        /*
         * Checkpoint执行流程：
         *
         * 1. 触发阶段：
         *    JobManager周期性触发Checkpoint
         *    ↓
         * 2. Barrier注入：
         *    Source接收到触发信号，向数据流注入Barrier
         *    ↓
         * 3. Barrier传播：
         *    Barrier随数据流向下游传播
         *    算子收到所有输入Barrier后，进行对齐（Exactly-Once模式）
         *    ↓
         * 4. 状态快照：
         *    算子将当前状态快照到StateBackend
         *    向下游发送Barrier
         *    ↓
         * 5. 完成确认：
         *    所有算子完成快照后，向JobManager确认
         *    JobManager标记Checkpoint完成
         *
         * 故障恢复流程：
         *
         * 1. 检测故障：
         *    TaskManager心跳超时或任务异常
         *    ↓
         * 2. 任务重启：
         *    根据重启策略重启任务
         *    ↓
         * 3. 状态恢复：
         *    从最近成功的Checkpoint恢复状态
         *    ↓
         * 4. 数据重放：
         *    Source从Checkpoint位置重新消费数据
         *    ↓
         * 5. 继续处理：
         *    任务恢复正常运行
         *
         * 生产环境配置建议：
         *
         * 1. Checkpoint间隔：
         *    - 实时性要求高：1-5分钟
         *    - 状态较大：5-10分钟
         *    - 平衡吞吐和延迟
         *
         * 2. StateBackend选择：
         *    - 状态 < 几百MB：HashMapStateBackend
         *    - 状态 > 1GB：EmbeddedRocksDBStateBackend
         *
         * 3. Checkpoint存储：
         *    - 使用高可用存储：HDFS、S3、OSS
         *    - 避免使用本地文件系统
         *
         * 4. 重启策略：
         *    - 优先使用失败率策略
         *    - 设置合理的失败阈值
         *
         * 5. 监控指标：
         *    - Checkpoint时长
         *    - Checkpoint大小
         *    - Checkpoint失败率
         *    - 状态增长趋势
         *
         * 6. Savepoint使用场景：
         *    - 版本升级
         *    - 集群迁移
         *    - 调整并行度
         *    - A/B测试
         *
         * 执行Savepoint：
         * bin/flink savepoint <jobId> [<savepointDirectory>]
         *
         * 从Savepoint恢复：
         * bin/flink run -s <savepointPath> <jobJar>
         */
    }
}

