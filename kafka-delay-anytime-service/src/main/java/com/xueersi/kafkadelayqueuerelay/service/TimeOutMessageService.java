package com.xueersi.kafkadelayqueuerelay.service;

import com.xueersi.kafkadelayqueuerelay.entity.DelayItem;
import com.xueersi.kafkaserviceapi.entity.Message;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.xueersi.kafkadelayqueuerelay.service.KafkaDelayQService.*;
import static com.xueersi.kafkaserviceapi.util.DelayMessageUtil.recalculateDelayMessage;

@Log4j2
@Service
public class TimeOutMessageService {
    @Autowired
    private MsgSendService msgSendService;

    @Async("noBoundTaskExecutor")
    public void handleTimeOutMsg(Map<TopicPartition, OffsetAndMetadata> offsetsMap, DelayQueue<DelayItem> delayQ
            , HashMap<String, AtomicInteger> topicMessageNum) {
        while (serviceIsNormal()) {
            try {
                DelayItem delayItem = delayQ.poll(1, TimeUnit.SECONDS);
                if (null == delayItem) {
                    continue;
                }
                TopicPartition topicPartition = delayItem.getTopicPartition();
                ConsumerRecord<String, String> consumerRecord = delayItem.getConsumerRecord();
                long offset = delayItem.getOffset();
                handleDelayMessage(consumerRecord);
                commitOffset(offsetsMap, topicPartition, offset);
                topicMessageNum.get(topicPartition.topic()).getAndDecrement();
            } catch (Exception e) {
                log.error("位移提交出现错误:", e);
            }
        }
    }


    private void handleDelayMessage(ConsumerRecord<String, String> record) {
        Message message = consumerRecord2Message(record);
        recalculateDelayMessage(message);
        msgSendService.sendMsg(message);
    }

    private Boolean serviceIsNormal() {
        return !SERVICE_STOP.get();
    }


}
