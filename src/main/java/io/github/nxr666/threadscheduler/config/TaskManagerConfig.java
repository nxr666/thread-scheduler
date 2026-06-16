package io.github.nxr666.threadscheduler.config;

import io.github.nxr666.threadscheduler.exception.TaskManagerException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Arrays;

@Slf4j
@Data
@ConfigurationProperties(prefix = "thread-scheduler")
public class TaskManagerConfig implements InitializingBean {
    private boolean enabled = false;
    private int totalThreads = 100;
    private int taskExecutorIdleTime = 300;
    private int scheduleRate = 300;
    private boolean adjustThreadsOff = false;
    private List<ExecutorConfig> executors;

    // JDK8兼容的不可变Set
    private static final Set<String> VALID_STRATEGIES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("AbortPolicy", "CallerRunsPolicy", "DiscardPolicy", "DiscardOldestPolicy"))
    );

    @Data
    public static class ExecutorConfig {
        private String name;
        private Integer coreSize;
        private Integer queueSize;
        private String rejectStrategy = "CallerRunsPolicy";
    }
    
    @Override
    public void afterPropertiesSet() {
        if(!enabled) {
            return;
        }
        log.info("开始初始化 TaskManagerConfig 配置");
        
        // 检查是否有执行器配置
        if (executors == null || executors.isEmpty()) {
            log.error("未定义执行器配置");
            throw new TaskManagerException("未定义执行器配置，请在配置文件中添加 task-manager.executors 配置");
        }
        
        // 检查执行器名称是否唯一
        Set<String> executorNames = new HashSet<>();
        for (ExecutorConfig executor : executors) {
            if (executor.getName() == null || executor.getName().trim().isEmpty()) {
                log.error("执行器名称不能为空");
                throw new TaskManagerException("执行器名称不能为空");
            }
            
            if (!executorNames.add(executor.getName())) {
                log.warn("执行器名称不唯一: {}", executor.getName());
                throw new TaskManagerException("执行器名称不唯一: " + executor.getName());
            }
        }
        
        // 非必填配置设置默认值
        setDefaultValues();
        
        // 判断各执行器配置的线程数之和是否大于总的线程数配置
        int totalConfiguredThreads = executors.stream()
                .mapToInt(e -> e.getCoreSize() != null ? e.getCoreSize() : Runtime.getRuntime().availableProcessors())
                .sum();
                
        if (totalConfiguredThreads > totalThreads) {
            log.warn("将最大线程数配置调整为各执行器之和，避免任务饥饿");
            this.totalThreads = totalConfiguredThreads;
            log.info("总线程数已调整为: {}", this.totalThreads);
        }
        
        // 输出告警日志
        log.info("TaskManagerConfig 初始化完成");
        log.info("总线程数: {}", totalThreads);
        log.info("调度频率: {}秒", scheduleRate);
        log.info("线程调整功能: {}", adjustThreadsOff ? "关闭" : "开启");
        log.info("配置的执行器数量: {}", executors.size());
        
        for (ExecutorConfig executor : executors) {
            log.info("执行器 [{}] - 核心线程数: {}, 队列大小: {}, 拒绝策略: {}", 
                    executor.getName(), executor.getCoreSize(), executor.getQueueSize(), executor.getRejectStrategy());
        }
    }
    
    /**
     * 设置非必填配置的默认值
     */
    private void setDefaultValues() {
        for (ExecutorConfig executor : executors) {
            // 设置默认核心线程数
            if (executor.getCoreSize() == null) {
                executor.setCoreSize(Runtime.getRuntime().availableProcessors());
                log.info("执行器 [{}] 核心线程数设置为默认值: {}", executor.getName(), executor.getCoreSize());
            }
            
            // 设置默认队列大小
            if (executor.getQueueSize() == null) {
                executor.setQueueSize(10000);
                log.info("执行器 [{}] 队列大小设置为默认值: {}", executor.getName(), executor.getQueueSize());
            }
            
            // 设置默认拒绝策略
            if (executor.getRejectStrategy() == null || executor.getRejectStrategy().trim().isEmpty()) {
                executor.setRejectStrategy("CallerRunsPolicy");
                log.info("执行器 [{}] 拒绝策略设置为默认值: {}", executor.getName(), executor.getRejectStrategy());
            }
            
            // 验证拒绝策略是否有效
            validateRejectStrategy(executor);
        }
    }
    
    /**
     * 验证拒绝策略是否有效
     */
    private void validateRejectStrategy(ExecutorConfig executor) {
        String strategy = executor.getRejectStrategy();
        if (strategy == null || strategy.trim().isEmpty()) {
            return; // 已在上面设置了默认值
        }
        // 忽略大小写查找匹配的策略
        String normalizedStrategy = null;
        for (String valid : VALID_STRATEGIES) {
            if (valid.equalsIgnoreCase(strategy.trim())) {
                normalizedStrategy = valid;
                break;
            }
        }
        if (normalizedStrategy != null) {
            // 找到匹配的策略，统一设置为标准格式
            executor.setRejectStrategy(normalizedStrategy);
            log.debug("执行器 [{}] 拒绝策略已标准化为: {}", executor.getName(), normalizedStrategy);
        } else {
            log.warn("执行器 [{}] 拒绝策略 [{}] 不是有效策略，将使用默认策略 CallerRunsPolicy", executor.getName(), strategy);
            executor.setRejectStrategy("CallerRunsPolicy");
        }
    }
}
