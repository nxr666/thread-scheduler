# Thread Scheduler

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-8%2B-orange.svg)](#环境要求)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-2.7.x-green.svg)](#环境要求)

一个面向 Spring Boot 的多线程任务调度 Starter。通过统一的「任务管理器 + 多执行器」模型集中管理多个线程池，支持按队列积压情况**动态调整线程资源**、空闲执行器**自动回收**，从而提升单机 CPU 资源利用率，并简化业务侧的线程池开发。

## 背景

在需要大量并行处理的场景（如数据解析、批量计算、IO 密集任务）中，业务方往往各自创建线程池，缺乏统一管理，容易出现资源争抢、利用率不足、配置分散等问题。本框架将线程资源集中托管，由调度器根据运行时负载在多个执行器之间动态分配线程，并自动回收空闲资源。

## 特性

- **多执行器统一管理**：通过配置定义多个命名执行器，按业务类型（CPU 密集、IO 密集、快速任务等）隔离。
- **动态线程调度**：定时根据各执行器队列积压情况，将空闲线程在执行器之间重新分配。
- **空闲自动回收**：执行器空闲超时后自动关闭并回收线程资源。
- **总量约束**：全局线程总数受控，避免线程无限膨胀。
- **完善的统计**：任务提交数、完成数、拒绝数、丢弃数、调用方执行数、成功率/丢弃率等，定时输出。
- **灵活的拒绝策略**：支持 `AbortPolicy`、`CallerRunsPolicy`、`DiscardPolicy`、`DiscardOldestPolicy`。
- **开箱即用**：基于 Spring Boot 自动配置，引入依赖 + 简单配置即可使用。

## 环境要求

- JDK 8 及以上
- Spring Boot 2.7.x（默认基于 2.7.18 构建）

## 快速开始

### 1. 引入依赖

```xml
<dependency>
    <groupId>io.github.nxr666</groupId>
    <artifactId>spring-boot-starter-thread-scheduler</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

> 当前为本地构建版本。可执行 `mvn clean install` 安装到本地仓库后使用。

### 2. 添加配置

在 `application.yml` 中配置任务管理器与执行器：

```yaml
thread-scheduler:
  enabled: true                 # 是否启用（必须为 true 才会自动装配）
  total-threads: 100            # 全局线程总数上限
  task-executor-idle-time: 300  # 执行器空闲超时时间（秒），超时后回收
  schedule-rate: 300            # 调度/监控频率（秒）
  adjust-threads-off: false     # 是否关闭动态线程调整（true=仅监控不调整）
  executors:
    - name: "cpu-executor"      # 执行器名称（唯一）
      core-size: 4              # 核心线程数
      queue-size: 100           # 队列容量
      reject-strategy: "AbortPolicy"

    - name: "io-executor"
      core-size: 20
      queue-size: 1000
      reject-strategy: "CallerRunsPolicy"

    - name: "quick-executor"
      core-size: 8
      queue-size: 2000
      reject-strategy: "DiscardOldestPolicy"
```

### 3. 在业务代码中使用

注入 `TaskManager`，向指定执行器提交任务：

```java
import io.github.nxr666.threadscheduler.core.TaskManager;
import io.github.nxr666.threadscheduler.task.RunnableTask;
import io.github.nxr666.threadscheduler.task.CallableTask;

@Service
public class DemoService {

    @Autowired
    private TaskManager taskManager;

    public void runAsync() {
        // 提交无返回值任务
        taskManager.execute("io-executor", new RunnableTask() {
            @Override
            protected void runTask() {
                // 业务逻辑
            }
        });
    }

    public String runWithResult() throws Exception {
        // 提交有返回值任务
        Future<String> future = taskManager.submit("cpu-executor", new CallableTask<String>() {
            @Override
            protected String callTask() throws Exception {
                return "result";
            }
        });
        return future.get();
    }
}
```

`RunnableTask` / `CallableTask` 内置任务状态（`CREATED`/`RUNNING`/`SUCCESS`/`FAILED`）、起止时间与异常信息的跟踪，可按需扩展 `handleException`。

## 配置项说明

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `thread-scheduler.enabled` | boolean | `false` | 是否启用框架，需为 `true` 才会自动装配 |
| `thread-scheduler.total-threads` | int | `100` | 全局线程总数上限（小于各执行器核心线程数之和时会自动上调） |
| `thread-scheduler.task-executor-idle-time` | int | `300` | 执行器空闲超时时间（秒） |
| `thread-scheduler.schedule-rate` | int | `300` | 调度与状态监控频率（秒） |
| `thread-scheduler.adjust-threads-off` | boolean | `false` | 关闭动态线程调整，仅保留状态监控 |
| `thread-scheduler.executors[].name` | string | - | 执行器名称，必须唯一 |
| `thread-scheduler.executors[].core-size` | int | CPU 核心数 | 执行器核心线程数 |
| `thread-scheduler.executors[].queue-size` | int | `10000` | 执行器任务队列容量 |
| `thread-scheduler.executors[].reject-strategy` | string | `CallerRunsPolicy` | 拒绝策略，见下表 |

### 拒绝策略

| 策略 | 行为 |
|------|------|
| `AbortPolicy` | 队列满时抛出 `RejectedExecutionException` |
| `CallerRunsPolicy` | 队列满时由提交任务的线程直接执行 |
| `DiscardPolicy` | 队列满时静默丢弃新任务 |
| `DiscardOldestPolicy` | 队列满时丢弃队首最旧任务后重试入队 |

> 策略名称大小写不敏感，无效策略将回退为 `CallerRunsPolicy`。

## 工作原理

- **任务管理器（TaskManager）**：对外统一入口，负责按名称查找/创建执行器、提交任务、关闭执行器，并维护全局线程占用与汇总统计。
- **任务执行器（TaskExecutor）**：对 `ThreadPoolExecutor` 的封装，提供线程数动态调整、运行指标采集与拒绝统计能力。
- **配置管理器（TaskManagerConfig）**：加载并校验配置（执行器名称唯一性、拒绝策略合法性、线程总量约束等）。
- **调度线程**：按 `schedule-rate` 周期执行：
  1. 关闭并回收空闲超时的执行器；
  2. 将可用空闲线程按各执行器队列积压比例补充给繁忙执行器；
  3. 输出执行器与全局汇总统计日志。

更详细的设计说明（架构图、类图、交互流程）见 [`docs/多线程调度框架设计.md`](docs/多线程调度框架设计.md)。

## 构建

```bash
# 编译并打包（跳过测试）
mvn clean package -DskipTests

# 安装到本地仓库
mvn clean install -DskipTests
```

### 运行测试

性能、压力、演示类测试（`DemoTest`、`StressTest`、`PerformanceTest`、`BenchmarkTest`、`MultiExecutorSchedulingTest`、`TaskStatisticsTest`）包含较长时间的压测与 `sleep`，已用 JUnit 5 的 `@Tag("slow")` 标记，**默认构建会自动跳过**，因此日常构建只需数秒。

```bash
# 默认：仅运行快速测试（跳过 slow），适合日常构建
mvn test

# 运行包含 slow 在内的全部测试（CI 或发布前）
mvn test -P all-tests

# 仅运行指定的某些测试类
mvn test "-Dtest=RejectionPolicyTest,TaskStatisticsTest"
```

> 实现方式：`pom.xml` 中 Surefire 默认 `excludedGroups=slow`，`all-tests` profile 会清空该排除项。新增耗时测试时，给测试类加上 `@Tag("slow")` 即可被默认构建跳过。

## 许可证

本项目基于 [Apache License 2.0](LICENSE) 开源。
