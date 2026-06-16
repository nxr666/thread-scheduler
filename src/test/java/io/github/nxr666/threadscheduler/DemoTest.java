package io.github.nxr666.threadscheduler;

import io.github.nxr666.threadscheduler.core.TaskManager;
import io.github.nxr666.threadscheduler.task.CallableTask;
import io.github.nxr666.threadscheduler.task.RunnableTask;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
@Tag("slow")
@SpringBootTest
public class DemoTest {

    @Autowired
    private TaskManager taskManager;

    @Test
    public void testDemo() throws Exception {
        log.info("=== 多线程调度框架演示开始 ===");

        // 演示基本用法
        basicUsageDemo();

        // 演示不同类型任务
        differentTaskTypesDemo();

        // 演示批量任务处理
        batchTaskProcessingDemo();

        // 演示异步任务处理
        asyncTaskProcessingDemo();

        // 演示错误处理
        errorHandlingDemo();

        log.info("=== 多线程调度框架演示结束 ===");
    }

    /**
     * 基本用法演示
     */
    private void basicUsageDemo() throws InterruptedException {
        log.info("--- 基本用法演示 ---");

        // 提交Runnable任务
        taskManager.execute("quick-executor", new RunnableTask() {
            @Override
            protected void runTask() {
                log.info("执行简单任务: {}", Thread.currentThread().getName());
            }
        });

        // 提交Callable任务
        Future<String> future = taskManager.submit("cpu-executor", new CallableTask<String>() {
            @Override
            protected String callTask() throws Exception {
                log.info("执行计算任务: {}", Thread.currentThread().getName());
                Thread.sleep(1000);
                return "计算结果: " + (Math.random() * 1000);
            }
        });

        try {
            String result = future.get(5, TimeUnit.SECONDS);
            log.info("任务执行结果: {}", result);
        } catch (Exception e) {
            log.error("任务执行失败", e);
        }

        Thread.sleep(2000);
    }

    /**
     * 不同类型任务演示
     */
    private void differentTaskTypesDemo() throws InterruptedException {
        log.info("--- 不同类型任务演示 ---");

        // CPU密集型任务
        taskManager.execute("cpu-executor", new RunnableTask() {
            @Override
            protected void runTask() {
                log.info("开始CPU密集型任务");
                long result = 0;
                for (int i = 0; i < 1000000; i++) {
                    result += Math.sqrt(i);
                }
                log.info("CPU密集型任务完成，结果: {}", result);
            }
        });

        // IO密集型任务
        taskManager.execute("io-executor", new RunnableTask() {
            @Override
            protected void runTask() {
                log.info("开始IO密集型任务");
                try {
                    // 模拟数据库查询或网络请求
                    Thread.sleep(2000);
                    log.info("IO密集型任务完成");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("IO任务被中断", e);
                }
            }
        });

        // 快速任务
        for (int i = 0; i < 10; i++) {
            final int taskId = i;
            taskManager.execute("quick-executor", new RunnableTask() {
                @Override
                protected void runTask() {
                    log.info("快速任务 {} 执行完成", taskId);
                }
            });
        }

        Thread.sleep(5000);
    }

    /**
     * 批量任务处理演示
     */
    private void batchTaskProcessingDemo() throws InterruptedException {
        log.info("--- 批量任务处理演示 ---");

        int batchSize = 50;
        List<Future<Integer>> futures = new ArrayList<>();

        // 提交批量计算任务
        for (int i = 0; i < batchSize; i++) {
            final int taskId = i;
            Future<Integer> future = taskManager.submit("cpu-executor", new CallableTask<Integer>() {
                @Override
                protected Integer callTask() throws Exception {
                    // 模拟计算任务
                    int result = 0;
                    for (int j = 0; j < 10000; j++) {
                        result += j * taskId;
                    }
                    return result;
                }
            });
            futures.add(future);
        }

        // 收集结果
        int totalResult = 0;
        int completedTasks = 0;
        for (Future<Integer> future : futures) {
            try {
                Integer result = future.get(10, TimeUnit.SECONDS);
                totalResult += result;
                completedTasks++;
            } catch (Exception e) {
                log.error("批量任务执行失败", e);
            }
        }

        log.info("批量任务处理完成，完成数量: {}/{}, 总结果: {}",
                completedTasks, batchSize, totalResult);

        Thread.sleep(2000);
    }

    /**
     * 异步任务处理演示
     */
    private void asyncTaskProcessingDemo() throws InterruptedException {
        log.info("--- 异步任务处理演示 ---");

        // 提交异步任务，不等待结果
        taskManager.execute("io-executor", new RunnableTask() {
            @Override
            protected void runTask() {
                log.info("异步任务开始执行");
                try {
                    // 模拟长时间运行的任务
                    for (int i = 0; i < 5; i++) {
                        Thread.sleep(1000);
                        log.info("异步任务进度: {}/5", i + 1);
                    }
                    log.info("异步任务执行完成");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("异步任务被中断", e);
                }
            }
        });

        // 主线程继续执行其他任务
        log.info("主线程继续执行其他操作...");

        // 提交更多快速任务
        for (int i = 0; i < 5; i++) {
            final int taskId = i;
            taskManager.execute("quick-executor", new RunnableTask() {
                @Override
                protected void runTask() {
                    log.info("主线程同时执行的快速任务 {}", taskId);
                }
            });
        }

        Thread.sleep(7000); // 等待异步任务完成
    }

    /**
     * 错误处理演示
     */
    private void errorHandlingDemo() throws InterruptedException {
        log.info("--- 错误处理演示 ---");

        // 提交会抛异常的任务
        Future<String> future = taskManager.submit("performance-executor", new CallableTask<String>() {
            @Override
            protected String callTask() throws Exception {
                log.info("执行可能出错的任务");

                // 模拟随机错误
                if (Math.random() < 0.5) {
                    throw new RuntimeException("模拟任务执行异常");
                }

                return "任务执行成功";
            }
        });

        try {
            String result = future.get(5, TimeUnit.SECONDS);
            log.info("任务执行成功: {}", result);
        } catch (Exception e) {
            log.error("任务执行出现异常: {}", e.getMessage());
        }

        // 提交多个任务，演示异常不会影响其他任务
        for (int i = 0; i < 10; i++) {
            final int taskId = i;
            taskManager.execute("performance-executor", new RunnableTask() {
                @Override
                protected void runTask() {
                    try {
                        if (taskId % 3 == 0) {
                            throw new RuntimeException("任务 " + taskId + " 故意抛出异常");
                        }
                        log.info("任务 {} 正常执行完成", taskId);
                    } catch (Exception e) {
                        log.error("任务 {} 执行异常: {}", taskId, e.getMessage());
                    }
                }
            });
        }

        Thread.sleep(3000);
    }
}
