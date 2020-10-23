package com.xueersi.kafkadelayqueuerelay.threadpool;

import com.xueersi.kafkadelayqueuerelay.config.threadpool.MaxFixedTaskThreadPoolProperties;
import com.xueersi.kafkadelayqueuerelay.config.threadpool.NoBoundTaskThreadPoolProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 创建线程池配置类
 */
@Configuration
public class TaskExecutePool {

    @Autowired
    private NoBoundTaskThreadPoolProperties noBoundTaskThreadPoolProperties;
    @Autowired
    private MaxFixedTaskThreadPoolProperties maxFixedTaskThreadPoolProperties;

    @Bean(name = "noBoundTaskExecutor")
    public Executor noBoundTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        //核心线程池大小
        executor.setCorePoolSize(noBoundTaskThreadPoolProperties.getCorePoolSize());
        //最大线程数
        executor.setMaxPoolSize(noBoundTaskThreadPoolProperties.getMaxPoolSize());
        //队列容量
        executor.setQueueCapacity(noBoundTaskThreadPoolProperties.getQueueCapacity());
        //活跃时间
        executor.setKeepAliveSeconds(noBoundTaskThreadPoolProperties.getKeepAliveSeconds());
        //线程名字前缀
        executor.setThreadNamePrefix("noBoundTaskExecutor-");

        // setRejectedExecutionHandler：当pool已经达到max size的时候，如何处理新任务
        // CallerRunsPolicy：不在新线程中执行任务，而是由调用者所在的线程来执行
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // 等待所有任务结束后再关闭线程池
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();
        return executor;
    }

    @Bean(name = "maxFixedTaskExecutor")
    public Executor maxFixedTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        //核心线程池大小
        executor.setCorePoolSize(maxFixedTaskThreadPoolProperties.getCorePoolSize());
        //最大线程数
        executor.setMaxPoolSize(maxFixedTaskThreadPoolProperties.getMaxPoolSize());
        //队列容量
        executor.setQueueCapacity(maxFixedTaskThreadPoolProperties.getQueueCapacity());
        //活跃时间
        executor.setKeepAliveSeconds(maxFixedTaskThreadPoolProperties.getKeepAliveSeconds());
        //线程名字前缀
        executor.setThreadNamePrefix("maxFixedTaskExecutor-");

        // setRejectedExecutionHandler：当pool已经达到max size的时候，如何处理新任务
        // CallerRunsPolicy：不在新线程中执行任务，而是由调用者所在的线程来执行
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // 等待所有任务结束后再关闭线程池
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();
        return executor;
    }

}