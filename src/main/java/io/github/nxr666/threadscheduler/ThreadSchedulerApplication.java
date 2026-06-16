package io.github.nxr666.threadscheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 多线程调度框架主启动类
 */
@SpringBootApplication
public class ThreadSchedulerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ThreadSchedulerApplication.class, args);
    }
} 