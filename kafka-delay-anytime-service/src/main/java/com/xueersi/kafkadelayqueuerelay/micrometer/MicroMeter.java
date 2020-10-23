package com.xueersi.kafkadelayqueuerelay.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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
     * 提交消息错误统计
     */
    public Counter getCommitMsgErrCounter() {
        Counter counter;
        String name = COMMIT_MSG_ERR;
        String key = getMonitorKey(COUNTER_SYMBOL, name);
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
                        .tags("func", name, "topic")
                        .description("提交消息错误计数器")
                        .register(registry);
                microMeterMap.put(key, counter);
            } finally {
                lock.unlock();
            }
        }
        return counter;
    }

    /**
     * 提交消息错误统计
     */
    public AtomicLong msgMapSizeGauge(String topic) {
        Gauge gauge;
        AtomicLong atomicLong;
        String gaugeKey = getMonitorKey(GAUGE_SYMBOL, topic, MSG_NUM_IN_MEM);
        String atomicKey = getMonitorKey(ATOMIC_LONG, topic, MSG_NUM_IN_MEM);
        String name = MSG_NUM_IN_MEM;
        if (microMeterMap.containsKey(atomicKey) && microMeterMap.containsKey(gaugeKey)) {
            atomicLong = (AtomicLong) microMeterMap.get(atomicKey);
        } else {
            try {
                lock.lock();
                atomicLong = new AtomicLong(0);
                gauge = Gauge.builder(name, atomicLong, AtomicLong::get)
                        .tags("func", name, "topic", topic)
                        .description("mem中消息的数量")
                        .register(registry);
                microMeterMap.put(atomicKey, atomicLong);
                microMeterMap.put(gaugeKey, gauge);
            } finally {
                lock.unlock();
            }
        }
        return atomicLong;
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



