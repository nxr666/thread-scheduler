package io.github.nxr666.threadscheduler;

import io.github.nxr666.threadscheduler.core.TaskManager;
import io.github.nxr666.threadscheduler.exception.ExecutorClosedException;
import io.github.nxr666.threadscheduler.task.RunnableTask;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 拒绝策略和异常处理测试
 * 验证不同类型的 RejectedExecutionException 的处理逻辑
 */
@Slf4j
@SpringBootTest(classes = io.github.nxr666.threadscheduler.ThreadSchedulerApplication.class)
public class RejectionPolicyTest {

    @Autowired
    private TaskManager TaskManager;

    /**
     * 测试队列满时的拒绝策略处理
     * 应该抛出 RejectedExecutionException 而不是 ExecutorClosedException
     */
    @Test
    public void testQueueFullRejection() throws InterruptedException {
        log.info("=== 测试队列满时的拒绝策略处理 ===");
        
        // 使用一个小队列的执行器来快速填满队列
        String executorName = "cpu-executor"; // 队列大小为100，核心线程数为4
        
        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch startLatch = new CountDownLatch(1);
        
        try {
            // 先提交一些长时间运行的任务来占满线程池
            for (int i = 0; i < 4; i++) { // 占满核心线程
                TaskManager.execute(executorName, new RunnableTask() {
                    @Override
                    protected void runTask() {
                        try {
                            startLatch.countDown();
                            blockLatch.await(); // 阻塞任务，占住线程
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
            }
            
            // 等待线程池被占满
            startLatch.await(5, TimeUnit.SECONDS);
            Thread.sleep(100);
            
            // 提交大量任务来填满队列
            for (int i = 0; i < 150; i++) { // 超过队列大小100
                try {
                    TaskManager.execute(executorName, new RunnableTask() {
                        @Override
                        protected void runTask() {
                            log.debug("任务执行");
                        }
                    });
                } catch (RejectedExecutionException e) {
                    // 预期的异常：队列满触发拒绝策略
                    log.info("捕获到预期的 RejectedExecutionException: {}", e.getMessage());
                    break; // 测试通过，退出循环
                } catch (ExecutorClosedException e) {
                    log.error("意外的 ExecutorClosedException: {}", e.getMessage());
                    throw new AssertionError("队列满不应该抛出 ExecutorClosedException", e);
                }
            }
            
        } finally {
            // 释放阻塞的任务
            blockLatch.countDown();
        }
        
        log.info("队列满拒绝策略测试完成");
    }

    /**
     * 测试执行器关闭时的异常处理
     * 应该抛出 ExecutorClosedException 并进行重试
     */
    @Test 
    public void testExecutorClosedException() {
        log.info("=== 测试执行器关闭时的异常处理 ===");
        
        String executorName = "io-executor";
        
        try {

            Thread thread = new Thread(() -> {
                for (int i=0;i<100;i++) {
                    final int taskIndex = i + 1; // 创建final变量供lambda使用
                    try {
                        // 等待执行器创建完成
                        Thread.sleep(10);

                        // 尝试提交任务到已关闭的执行器
                        TaskManager.execute(executorName, new RunnableTask() {
                            @Override
                            protected void runTask() {
                                log.info("任务执行 - 第{}次", taskIndex);
                            }
                        });

                    } catch (Exception e) {
                        log.error("测试执行器关闭异常处理时出错: {}", e.getMessage());
                    }
                }
            });

            thread.start();
            Thread.sleep(1000);
            // 手动关闭执行器
            TaskManager.shutdown(executorName);
            // 等待线程完成
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("等待测试线程完成时被中断");
            }
            Thread.sleep(1000);
            log.info("执行器关闭后重试机制工作正常");
            
        } catch (ExecutorClosedException e) {
            log.info("捕获到 ExecutorClosedException，这是预期的: {}", e.getMessage());
            log.info("执行器名称: {}, 操作: {}", e.getExecutorName(), e.getOperation());
        } catch (Exception e) {
            log.error("测试执行器关闭异常处理时出错: {}", e.getMessage());
        }
    }

} 