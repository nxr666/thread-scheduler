package io.github.nxr666.threadscheduler;

import io.github.nxr666.threadscheduler.core.TaskManager;
import io.github.nxr666.threadscheduler.task.RunnableTask;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 任务统计功能测试
 * 验证总任务数、丢弃任务数、主线程执行任务数等统计信息
 */
@Slf4j
@Tag("slow")
@SpringBootTest
public class TaskStatisticsTest {

    @Autowired
    private TaskManager TaskManager;

    /**
     * 测试基本的任务统计功能
     */
    @Test
    public void testBasicTaskStatistics() throws InterruptedException {
        log.info("=== 基本任务统计功能测试 ===");
        
        String executorName = "cpu-executor";
        CountDownLatch latch = new CountDownLatch(10);
        
        // 提交一些正常任务
        for (int i = 0; i < 10; i++) {
            final int taskId = i;
            TaskManager.execute(executorName, new RunnableTask() {
                @Override
                protected void runTask() {
                    try {
                        Thread.sleep(50); // 模拟任务执行
                        log.debug("任务 {} 执行完成", taskId);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        
        // 等待任务完成
        latch.await(10, TimeUnit.SECONDS);
        
        // 等待一下让统计数据更新
        Thread.sleep(10000);
        
        log.info("基本任务统计测试完成，查看日志获取统计信息");
    }

    /**
     * 测试CallerRunsPolicy的统计
     * 需要填满队列并触发CallerRunsPolicy
     */
    @Test
    public void testCallerRunsPolicyStatistics() throws InterruptedException {
        log.info("=== CallerRunsPolicy统计测试 ===");
        
        // 使用io-executor，它配置了CallerRunsPolicy
        String executorName = "io-executor";
        // 先提交足够多的长时间任务来填满线程池和队列
        for (int i = 0; i < 2000; i++) { // 填满核心线程
            TaskManager.execute(executorName, new RunnableTask() {
                @Override
                protected void runTask() {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }
        // 等待一段时间让统计数据更新
        Thread.sleep(10000);
        
        log.info("CallerRunsPolicy统计测试完成");
    }

    /**
     * 测试DiscardPolicy的统计
     */
    @Test
    public void testDiscardPolicyStatistics() throws InterruptedException {
        log.info("=== DiscardPolicy统计测试 ===");
        
        // 使用quick-executor，它配置了DiscardOldestPolicy
        String executorName = "quick-executor";
        for (int i = 0; i < 2000; i++) { // 填满核心线程
            TaskManager.execute(executorName, new RunnableTask() {
                @Override
                protected void runTask() {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }
        // 等待一段时间让统计数据更新
        Thread.sleep(10000);
        
        log.info("DiscardPolicy统计测试完成");
    }

} 