package com.xueersi.kafkadelayqueuerelay.service;

import com.xueersi.kafkaserviceapi.entity.Message;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import static com.xueersi.kafkadelayqueuerelay.service.KafkaDelayQService.msg2ProducerRecord;

@Log4j2
@Service
public class MsgSendService {
    @Resource(name = "asyncKafkaTemplate")
    private KafkaTemplate<String, String> asyncKafkaTemplate;

    public void sendMsg(Message message) {
        ProducerRecord<String, String> producerRecord = msg2ProducerRecord(message);
        asyncSendMsg(producerRecord);
    }

    private void asyncSendMsg(ProducerRecord<String, String> producerRecord) {
        asyncKafkaTemplate.send(producerRecord);
    }

}
