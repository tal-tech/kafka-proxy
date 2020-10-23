package com.xueersi.kafkaproducerservice.service;

import com.xueersi.kafkaproducerservice.micrometer.MicroMeter;
import com.xueersi.kafkaserviceapi.entity.Message;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.xueersi.kafkaserviceapi.constant.DelayConst.DELAY_PREFIX;
import static com.xueersi.kafkaserviceapi.constant.MessageParaConst.*;
import static com.xueersi.kafkaserviceapi.util.DelayMessageUtil.getDelayLevel;

@Log4j2
@Service
@EnableScheduling
public class MsgHandlerService {

    public static AtomicBoolean SERVICE_STOP = new AtomicBoolean(false);
    private Set<String> topicSet = new HashSet<>();
    @Autowired
    MicroMeter microMeter;
    @Autowired
    MsgSendService msgSendService;
    @Autowired
    MsgResendService msgResendService;

    @Value("${kafka.producer.servers}")
    private String servers;
    private final ReentrantLock lock = new ReentrantLock();

    @Scheduled(fixedRate = 120 * 1000)
    public synchronized void updateTopicList() {
        Properties props = new Properties();
        // 只需要提供一个或多个 broker 的 IP 和端口
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        try (AdminClient client = KafkaAdminClient.create(props)) {
            // 获取 topic 列表
            Set<String> topicsSet = client.listTopics().names().get();
            if (topicsSet == null || topicsSet.size() == 0) {
                return;
            }
            if (!topicSet.equals(topicsSet)) {
                topicSet = topicsSet;
                //初始化topic的错误监控
                log.info("topic更新:topicsSet:{}", topicsSet);
            }
        } catch (Exception e) {
            log.error(e);
        }
    }

    public static ProducerRecord<String, String> msg2ProducerRecord(Message message) {

        if (message.getMid() == null || message.getMid().equals("")) {
            message.setMid(UUID.randomUUID().toString());
        }
        if (message.getProducerIP() == null) {
            message.setProducerIP("");
        }
        byte[] ipBytes = message.getProducerIP().getBytes();
        ProducerRecord<String, String> producerRecord;

        ArrayList<Header> headers = new ArrayList<>();
        if (message.getHeaders() != null && message.getHeaders().size() > 0) {
            message.getHeaders().forEach((headerKey, headerValue) -> headers.add(new Header() {
                @Override
                public String key() {
                    return headerKey;
                }

                @Override
                public byte[] value() {
                    return headerValue.toString().getBytes();
                }
            }));
        }

        if (!message.getHeaders().containsKey(PRODUCER_ID)) {
            headers.add(new Header() {
                @Override
                public String key() {
                    return PRODUCER_ID;
                }

                @Override
                public byte[] value() {
                    return ipBytes;
                }
            });
        }

        if (!message.getHeaders().containsKey(MSG_ID)) {
            headers.add(new Header() {
                @Override
                public String key() {
                    return MSG_ID;
                }

                @Override
                public byte[] value() {
                    return message.getMid().getBytes();
                }
            });
        }

        if (message.getKey() == null || "".equals(message.getKey().trim())) {
            final String key = null;
            producerRecord = new ProducerRecord<>(message.getTopic(), null, key, message.getPayload(), headers);
        } else {
            producerRecord = new ProducerRecord<>(message.getTopic(), null, message.getKey(), message.getPayload(), headers);
        }
        return producerRecord;
    }

    public void checkDelayMessage(Message message) {
        if (message.getHeaders() == null) {
            message.setHeaders(new HashMap<>());
        }
        if (!message.getHeaders().containsKey(X_DELAY)) {
            return;
        }
        //延时时间,单位秒
        int delayTime;
        delayTime = Integer.parseInt(String.valueOf(message.getHeaders().get(X_DELAY)));

        //计算延时级别
        Integer delayLevel = getDelayLevel(delayTime);
        if (delayLevel == -1) {
            log.error("message:{}延时时间:{}传入有误,不能进入延时队列", message, delayTime);
            return;
        }
        message.getHeaders().put(EXPIRE_TIME_IN_TOPIC, System.currentTimeMillis() + delayLevel * 1000);
        message.getHeaders().put(MESSAGE_EXPIRE_TIME, System.currentTimeMillis() + delayTime * 1000);
        message.getHeaders().put(ORIGIN_TOPIC, message.getTopic());
        log.info("msg 延时时间为:{},被重新投递到新的延时队列中{}...", delayTime, DELAY_PREFIX + delayLevel);
        message.setTopic(DELAY_PREFIX + delayLevel);
    }

    public boolean isTopicNotCreated(String topic) {
        if (topicSet.contains(topic)) {
            return false;
        }
        boolean isGotLock = false;
        try {
            isGotLock = lock.tryLock(2, TimeUnit.SECONDS);
            if (!isGotLock) {
                return true;
            }
            if (topicSet.contains(topic)) {
                return false;
            }
            updateTopicList();
        } catch (InterruptedException e) {
            log.error(e);
        } finally {
            if (isGotLock) {
                lock.unlock();
            }
        }
        return !topicSet.contains(topic);
    }

}
