package com.xueersi.kafkaproducerservice.listener;

import com.xueersi.kafkaproducerservice.micrometer.MicroMeter;
import com.xueersi.kafkaproducerservice.service.MsgResendService;
import com.xueersi.kafkaproducerservice.util.LocalDbUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

import static com.xueersi.kafkaserviceapi.constant.MessageParaConst.MSG_ID;

/**
 * @author wudi
 * @version 1.0 2019/12/18
 */
@Log4j2
@Component
public class AsyncKafkaSendResultListener implements ProducerListener<String, String> {
    @Autowired
    MicroMeter microMeter;
    @Autowired
    @Lazy
    MsgResendService msgResendService;
    @Autowired
    LocalDbUtil localDbUtil;

    @Override
    public void onSuccess(ProducerRecord<String, String> producerRecord, RecordMetadata recordMetadata) {
        String msgId = getMsgId(producerRecord);
        localDbUtil.getMap().remove(msgId);
        microMeter.getTopicSendCounter(producerRecord.topic()).increment();
    }

    @Override
    public void onError(ProducerRecord<String, String> producerRecord, Exception exception) {
        if (exception != null) {
            String msgId = getMsgId(producerRecord);
            msgResendService.getExpireMap().put(msgId, "");
            microMeter.getAsySentErrorCounter(producerRecord.topic()).increment();
            log.error("消息发送到kafka失败{}", producerRecord, exception);
        }
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
