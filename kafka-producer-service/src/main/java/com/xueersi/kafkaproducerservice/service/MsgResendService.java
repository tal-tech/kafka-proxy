package com.xueersi.kafkaproducerservice.service;

import com.xueersi.kafkaproducerservice.util.LocalDbUtil;
import com.xueersi.kafkaserviceapi.entity.Message;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

import static com.xueersi.kafkaserviceapi.util.DelayMessageUtil.recalculateDelayMessage;

@Data
@Log4j2
@Service
public class MsgResendService {
    @Autowired
    MsgSendService msgSendService;
    @Autowired
    private LocalDbUtil localDbUtil;

    private ExpiringMap<String, String> expireMap = ExpiringMap.builder()
            .maxSize(1000000)
            .expiration(10, TimeUnit.SECONDS)
            .expirationPolicy(ExpirationPolicy.CREATED)
            .asyncExpirationListener((id, status) -> resendMsg((String) id))
            .build();

    public void resendMsg(String msgId) {
        Object messageObj = localDbUtil.getMap().get(msgId);
        if (null != messageObj) {
            Message message = recalculateDelayMessage((Message) messageObj);
            log.warn("检测到有消息未发送成功 ,将重新发送消息Id:{}", msgId);
            msgSendService.sendMsg(message);
        }
    }

    @PostConstruct
    public void getSendFailedMsgs() {
        if (localDbUtil.getMap().size() > 0) {
            localDbUtil.getMap().keySet().forEach(key -> {
                expireMap.put((String) key, "");
            });
        }
    }
}
