package com.xueersi.kafkadelayqueuerelay.listener;

import com.xueersi.kafkadelayqueuerelay.micrometer.MicroMeter;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

import static com.xueersi.kafkaserviceapi.constant.MessageParaConst.MSG_ID;

@Log4j2
@Component
public class AsyncKafkaSendResultListener implements ProducerListener<String, String> {
    @Autowired
    private MicroMeter microMeter;

    @Override
    public void onSuccess(ProducerRecord<String, String> producerRecord, RecordMetadata recordMetadata) {
        String msgId = getMsgId(producerRecord);
        microMeter.getTopicSendCounter(producerRecord.topic()).increment();
        log.info("发往topic:{}的消息成功,消息Id:{}", producerRecord.topic(), msgId);
    }

    @Override
    public void onError(ProducerRecord<String, String> producerRecord, Exception exception) {
        microMeter.getAsySentErrorCounter(producerRecord.topic()).increment();
        //异步发送改为同步发送
        log.error("消息异步发送失败,转为本地存储:{} ", producerRecord.toString(), exception);
    }

    private String getMsgId(ProducerRecord<String, String> producerRecord) {
        Headers headers = producerRecord.headers();
        for (Header header : headers) {
            if (MSG_ID.equals(header.key())) {
                return new String(header.value());
            }
        }
        return "";
    }

}
