package com.xueersi.kafkaproducerservice.service;

import com.xueersi.kafkaproducerservice.util.LocalDbUtil;
import com.xueersi.kafkaserviceapi.entity.Message;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import static com.xueersi.kafkaproducerservice.service.MsgHandlerService.msg2ProducerRecord;

@Log4j2
@Service
public class MsgSendService {
    @Resource(name = "asyncKafkaTemplate")
    private KafkaTemplate<String, String> asyncKafkaTemplate;
    @Autowired
    private LocalDbUtil localDbUtil;

    @Async("maxFixedTaskExecutor")
    public void sendMsg(Message message) {
        ProducerRecord<String, String> producerRecord = msg2ProducerRecord(message);
        localDbUtil.getMap().put(message.getMid(), message);
        asyncSendMsg(producerRecord);
    }

    private void asyncSendMsg(ProducerRecord<String, String> producerRecord) {
        asyncKafkaTemplate.send(producerRecord);
    }

}
