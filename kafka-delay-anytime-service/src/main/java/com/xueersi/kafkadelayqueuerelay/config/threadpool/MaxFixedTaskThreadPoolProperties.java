package com.xueersi.kafkadelayqueuerelay.config.threadpool;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 类说明：自定义线程池中线程的创建方式，把线程设置为守护线程
 */

@Data
@Component
@ConfigurationProperties(prefix = "task.max-fixed.pool")
public class MaxFixedTaskThreadPoolProperties {
    private int corePoolSize;

    private int maxPoolSize;

    private int keepAliveSeconds;

    private int queueCapacity;
}