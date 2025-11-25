package com.kinch.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink快速开始示例
 * <p>
 * 这是一个最简单的Flink程序，用于验证环境配置是否正确
 * <p>
 * 运行此程序，如果看到输出结果，说明Flink环境配置成功！
 */
public class QuickStartDemo {

    public static void main(String[] args) throws Exception {

        System.out.println("=".repeat(60));
        System.out.println("欢迎使用Flink生产级别学习项目！");
        System.out.println("=".repeat(60));

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        System.out.println("✅ 执行环境创建成功");

        // 2. 创建数据源
        DataStream<String> stream = env.fromElements(
                "Hello Flink!",
                "欢迎学习Flink",
                "这是一个生产级别的学习项目",
                "包含13个核心模块",
                "涵盖所有重要知识点"
        );

        System.out.println("✅ 数据源创建成功");

        // 3. 数据转换
        DataStream<String> resultStream = stream
                .map(line -> "【Flink处理】" + line)
                .name("Map-Transformation");

        System.out.println("✅ 数据转换配置成功");

        // 4. 输出结果
        resultStream.print();

        System.out.println("✅ 输出配置成功");

        // 5. 执行任务
        System.out.println("\n开始执行Flink任务...\n");
        System.out.println("-".repeat(60));

        env.execute("Flink Quick Start Demo");

        System.out.println("-".repeat(60));
        System.out.println("\n✅ 任务执行完成！");
        System.out.println("\n如果你看到了上面的输出结果，说明Flink环境配置成功！");
        System.out.println("\n接下来你可以：");
        System.out.println("1. 查看 README.md 了解项目结构");
        System.out.println("2. 查看 运行指南.md 学习如何运行各个Demo");
        System.out.println("3. 查看 核心概念总结.md 学习Flink核心知识");
        System.out.println("4. 按照学习路径逐个运行13个Demo");
        System.out.println("\n祝学习愉快！");
        System.out.println("=".repeat(60));
    }
}

