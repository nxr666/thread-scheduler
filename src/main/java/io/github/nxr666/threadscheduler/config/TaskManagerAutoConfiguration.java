package io.github.nxr666.threadscheduler.config;

import io.github.nxr666.threadscheduler.core.DefaultTaskManager;
import io.github.nxr666.threadscheduler.core.TaskManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * TaskManager自动配置类
 * 当配置了thread-scheduler相关属性时自动加载
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(TaskManagerConfig.class)
@ConditionalOnProperty(prefix = "thread-scheduler", name = "enabled", havingValue = "true")
public class TaskManagerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(TaskManager.class)
    public TaskManager taskManager(TaskManagerConfig taskManagerConfig) {
        DefaultTaskManager taskManager = new DefaultTaskManager();
        taskManager.setProperties(taskManagerConfig);
        return taskManager;
    }
} 