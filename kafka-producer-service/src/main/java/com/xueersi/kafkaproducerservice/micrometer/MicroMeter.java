package com.xueersi.kafkaproducerservice.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import static com.xueersi.kafkaserviceapi.constant.MicrometerConst.*;

@Component
public class MicroMeter {
    private final ConcurrentHashMap<String, Object> microMeterMap = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();
    @Autowired
    private MeterRegistry registry;

    /**
     * 异步发送到server失败统计
     */
    public Counter getAsySentErrorCounter(String topic) {
        Counter counter;
        String name = PRODUCER_ASY_ERR;
        String key = getMonitorKey(COUNTER_SYMBOL, name, topic);
        if (microMeterMap.containsKey(key)) {
            counter = (Counter) microMeterMap.get(key);
        } else {
            try {
                lock.lock();
                if (microMeterMap.containsKey(key)) {
                    counter = (Counter) microMeterMap.get(key);
                    return counter;
                }
                counter = Counter.builder(name)
                        .tags("func", name, "topic", topic)
                        .description("异步发送错误的计数器")
                        .register(registry);
                microMeterMap.put(key, counter);
            } finally {
                lock.unlock();
            }
        }
        return counter;
    }

    /**
     * topic不存在的错误统计
     */
    public Counter getTopicNotExistErrorCounter(String topic) {
        Counter counter;
        String name = TOPIC_NOT_EXIST_ERR;
        String key = getMonitorKey(COUNTER_SYMBOL, name, topic);
        if (microMeterMap.containsKey(key)) {
            counter = (Counter) microMeterMap.get(key);
        } else {
            try {
                lock.lock();
                if (microMeterMap.containsKey(key)) {
                    counter = (Counter) microMeterMap.get(key);
                    return counter;
                }
                counter = Counter.builder(name)
                        .tags("func", name, "topic", topic)
                        .description("topic不存在错误")
                        .register(registry);
                microMeterMap.put(key, counter);
            } finally {
                lock.unlock();
            }
        }
        return counter;
    }

    /**
     * IP 发送统计
     */
    public Counter getIpSendMsgCounter(String producerIP, String topic) {
        Counter counter;
        String name = IP_SEND_MSG;
        String key = getMonitorKey(COUNTER_SYMBOL, name, topic, producerIP);
        if (microMeterMap.containsKey(key)) {
            counter = (Counter) microMeterMap.get(key);
        } else {
            try {
                lock.lock();
                if (microMeterMap.containsKey(key)) {
                    counter = (Counter) microMeterMap.get(key);
                    return counter;
                }
                counter = Counter.builder(name)
                        .tags("func", name, "topic", topic, "producerIP", producerIP)
                        .description("业务方发送消息统计")
                        .register(registry);
                microMeterMap.put(key, counter);
            } finally {
                lock.unlock();
            }
        }
        return counter;
    }

    /**
     * 每个topic发送统计
     */
    public Counter getTopicSendCounter(String topic) {
        Counter counter;
        String name = TOPIC_SEND_NUM;
        String key = getMonitorKey(COUNTER_SYMBOL, name, topic);
        if (microMeterMap.containsKey(key)) {
            counter = (Counter) microMeterMap.get(key);
        } else {
            try {
                lock.lock();
                if (microMeterMap.containsKey(key)) {
                    counter = (Counter) microMeterMap.get(key);
                    return counter;
                }
                counter = Counter.builder(name)
                        .tags("func", name, "topic", topic)
                        .description("topic发送总数")
                        .register(registry);
                microMeterMap.put(key, counter);
            } finally {
                lock.unlock();
            }
        }
        return counter;
    }

    /**
     * 自定义监控map中的key
     */
    private String getMonitorKey(String type, String desc, String... args) {
        StringBuilder key = new StringBuilder();
        //分隔符
        String keySplitter = "_";
        if (null != type) {
            key.append(type);
            key.append(keySplitter);
        }
        for (String arg : args) {
            key.append(arg);
            key.append(keySplitter);
        }
        if (null != desc) {
            key.append(desc);
        }
        return key.toString();
    }
}



