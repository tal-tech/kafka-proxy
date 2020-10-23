package com.xueersi.kafkadelayqueuerelay.service;

import com.xueersi.kafkadelayqueuerelay.entity.DelayItem;
import com.xueersi.kafkadelayqueuerelay.micrometer.MicroMeter;
import com.xueersi.kafkaserviceapi.entity.Message;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.xueersi.kafkaserviceapi.constant.DelayConst.DELAY_RANK;
import static com.xueersi.kafkaserviceapi.constant.MessageParaConst.*;
import static com.xueersi.kafkaserviceapi.util.DelayMessageUtil.recalculateDelayMessage;

@Log4j2
@Service
public class KafkaDelayQService {
    //全局暂停业务
    public static AtomicBoolean SERVICE_STOP = new AtomicBoolean(false);
    @Autowired
    private TimeOutMessageService timeOutMessageService;
    @Autowired
    private MsgSendService msgSendService;
    @Autowired
    private MicroMeter microMeter;


    @Value("${kafka.bootstrap-servers}")
    private String servers;
    @Value("${kafka.consumer.enable-auto-commit}")
    private boolean enableAutoCommit;
    @Value("${kafka.consumer.session-timeout}")
    private String sessionTimeout;
    @Value("${kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;
    @Value("${costomize.msgMap.max-size}")
    private int msgMapMaxSize;
    @Value("${costomize.msgMap.min-size}")
    private int msgMapMinSize;


    @Async("noBoundTaskExecutor")
    public void consumerMsg(String topicPrefix, DelayQueue<DelayItem> delayQueue, String hostPort) {
        HashMap<String, AtomicInteger> messageNumMap = new HashMap<>();
        LinkedList<String> topicList = getTopicList(topicPrefix, messageNumMap);
        log.info("开启消费线程,消费topicList:{}", topicList);
        Properties properties = getConsumerProperties(topicPrefix, hostPort);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            Map<TopicPartition, OffsetAndMetadata> offsetsMap = new HashMap<>();
            ConsumerRebalanceListener consumerRebalanceListener = getRebalanceListener(consumer, topicList, offsetsMap, delayQueue, messageNumMap);
            consumer.subscribe(topicList, consumerRebalanceListener);
            //异步消费
            timeOutMessageService.handleTimeOutMsg(offsetsMap, delayQueue, messageNumMap);
            OffsetCommitCallback offsetCommitCallback = getCommitCallBack();
            while (serviceIsNormal()) {
                Set<String> toPauseSet = new HashSet<>();
                Set<String> toResumeSet = new HashSet<>();
                for (String topic : topicList) {
                    int topicMsgNum = messageNumMap.get(topic).get();
                    microMeter.msgMapSizeGauge(topic).set(topicMsgNum);
                    if (topicMsgNum > msgMapMaxSize) {
                        toPauseSet.add(topic);
                    } else if (topicMsgNum <= msgMapMinSize) {
                        toResumeSet.add(topic);
                    }
                }
                pauseFetch(consumer, toPauseSet);
                resumeFetch(consumer, toResumeSet);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                synchronized (offsetsMap) {
                    if (!offsetsMap.isEmpty()) {
                        consumer.commitAsync(offsetsMap, offsetCommitCallback);
                        offsetsMap.clear();
                    }
                }
                if (records.isEmpty()) {
                    continue;
                }
                handleDelayMessage(records, delayQueue, offsetsMap, messageNumMap);
            }
        } catch (Exception e) {
            log.error("topic:{},拉取消息发生错误", topicPrefix, e);
        }
    }

    private LinkedList<String> getTopicList(String topicPrefix, HashMap<String, AtomicInteger> topicMessageNum) {
        LinkedList<String> topicList = new LinkedList<>();
        for (int i = 0; i < DELAY_RANK; i++) {
            StringBuilder numZero = new StringBuilder();
            for (int j = 0; j < i; j++) {
                numZero.append("0");
            }
            String topic = topicPrefix + numZero;
            topicList.add(topic);
            topicMessageNum.put(topic, new AtomicInteger(0));
        }
        return topicList;
    }


    private void handleDelayMessage(ConsumerRecords<String, String> consumerRecords, DelayQueue<DelayItem> delayQ
            , Map<TopicPartition, OffsetAndMetadata> offsetsMap, HashMap<String, AtomicInteger> messageNum) {
        for (ConsumerRecord<String, String> record : consumerRecords) {
            log.info("---> record is " + record);
            String topic = record.topic();
            int partition = record.partition();
            long offset = record.offset();

            TopicPartition topicPartition = new TopicPartition(topic, partition);
            Map<String, Object> headers = new HashMap<>();
            record.headers().forEach(header -> headers.put(header.key(), new String(header.value())));
            long expireTimeInTopic = Long.parseLong((String) headers.remove(EXPIRE_TIME_IN_TOPIC));
            long messageExpireTime = Long.parseLong((String) headers.get(MESSAGE_EXPIRE_TIME));
            long nowTimestamp = System.currentTimeMillis();
            int delayTime = new Long((messageExpireTime - nowTimestamp) / 1000).intValue();
            //如果消息已经过期,或者在该级别的topic的延时时间已经被耗尽
            if (delayTime < 1 || expireTimeInTopic <= nowTimestamp) {
                Message message = consumerRecord2Message(record);
                //如果消息当前时间戳比 消息过期时间大,则把消息直接投递到初始的topic中
                recalculateDelayMessage(message);
                msgSendService.sendMsg(message);
                commitOffset(offsetsMap, topicPartition, offset);
            } else {
                //如果消息在topic中的过期时间戳比当前时间戳大 , 则把消息放入到 内存delayqueue中,倒计时
                log.info("msg从topic:{}中取出,消息剩余时间:{},消息被放入内存中倒计时!", topic, delayTime);
                delayQ.offer(new DelayItem(expireTimeInTopic, record, topicPartition, offset));
                messageNum.get(topic).getAndIncrement();
            }
        }
    }

    public static void commitOffset(Map<TopicPartition, OffsetAndMetadata> offsetsMap, TopicPartition topicPartition, long offset) {
        synchronized (offsetsMap) {
            if (!offsetsMap.containsKey(topicPartition)) {
                offsetsMap.put(topicPartition, new OffsetAndMetadata(offset + 1));
            } else {
                long position = offsetsMap.get(topicPartition).offset();
                if (position < offset + 1) {
                    offsetsMap.put(topicPartition, new OffsetAndMetadata(offset + 1));
                }
            }
        }
    }

    public static ProducerRecord<String, String> msg2ProducerRecord(Message message) {

        if (message.getMid() == null || message.getMid().equals("")) {
            message.setMid(UUID.randomUUID().toString());
        }
        if (message.getProducerIP() == null) {
            message.setProducerIP("");
        }

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

        String key = (message.getKey() == null || message.getKey().trim().equals("")) ? null : message.getMid();
        return new ProducerRecord<>(message.getTopic(), null, key, message.getPayload(), headers);
    }

    public static Message consumerRecord2Message(ConsumerRecord<String, String> record) {
        String payload = record.value();
        String key = record.key();
        String topic = record.topic();
        Map<String, Object> headers = new HashMap<>();
        record.headers().forEach(header -> headers.put(header.key(), new String(header.value())));
        Message message = new Message();
        message.setHeaders(headers);
        if (message.getHeaders().containsKey(MSG_ID)) {
            message.setMid((String) message.getHeaders().get(MSG_ID));
        } else {
            message.setMid(UUID.randomUUID().toString());
        }
        message.setKey(key);
        message.setPayload(payload);
        message.setTopic(topic);
        return message;
    }

    public Properties getConsumerProperties(String topicPrefix, String hostPort) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StickyAssignor.class.getName());
        String delayConsumerStr = "mqp-delay-consumer-";
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, delayConsumerStr + topicPrefix);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, delayConsumerStr + topicPrefix + "~" + hostPort);

        return properties;
    }

    private ConsumerRebalanceListener getRebalanceListener(KafkaConsumer<String, String> consumer, LinkedList<String> topicList
            , Map<TopicPartition, OffsetAndMetadata> offsetsMap, DelayQueue<DelayItem> delayQueue, HashMap<String, AtomicInteger> messageNumMap) {
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                log.info("分区再均衡开始,分区再均衡前获得的分区:{}", collection);
                Set<String> topicSet = new HashSet<>(topicList);
                pauseFetch(consumer, topicSet);
                //同步提交
                synchronized (offsetsMap) {
                    if (!offsetsMap.isEmpty()) {
                        consumer.commitSync(offsetsMap);
                        offsetsMap.clear();
                    }
                }
                //清理缓存
                delayQueue.clear();
                for (String topic : messageNumMap.keySet()) {
                    messageNumMap.get(topic).set(0);
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                log.info("分区再均衡结束,分区再均衡后获得的分区:{}", collection);
            }
        };
    }

    private void pauseFetch(KafkaConsumer<String, String> consumer, Set<String> topicSet) {
        Set<TopicPartition> toPausedTPs = new HashSet<>();
        for (TopicPartition assignTP : consumer.assignment()) {
            if (topicSet.contains(assignTP.topic())) {
                toPausedTPs.add(assignTP);
            }
        }
        toPausedTPs.removeAll(consumer.paused());
        if (toPausedTPs.size() > 0) {
            consumer.pause(toPausedTPs);
            log.info("暂停拉取:{}", consumer.paused());
        }
    }

    private void resumeFetch(KafkaConsumer<String, String> consumer, Set<String> topicSet) {
        Set<TopicPartition> toResumeTPs = new HashSet<>();
        for (TopicPartition pausedTP : consumer.paused()) {
            if (topicSet.contains(pausedTP.topic())) {
                toResumeTPs.add(pausedTP);
            }
        }
        toResumeTPs.retainAll(consumer.assignment());
        if (toResumeTPs.size() > 0) {
            consumer.resume(toResumeTPs);
            log.info("恢复拉取:{}", toResumeTPs);
        }
    }

    private OffsetCommitCallback getCommitCallBack() {
        return (offsets, exception) -> {
            if (exception == null) {
                log.info("成功提交位移:{}", offsets);
            } else {
                microMeter.getCommitMsgErrCounter().increment();
                log.error("fail to commit offsets {}", offsets, exception);
            }
        };
    }

    private Boolean serviceIsNormal() {
        return !SERVICE_STOP.get();
    }

}
