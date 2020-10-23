package com.xueersi.kafkaserviceapi.util;

import com.xueersi.kafkaserviceapi.entity.Message;
import lombok.extern.log4j.Log4j2;

import static com.xueersi.kafkaserviceapi.constant.DelayConst.DELAY_PREFIX;
import static com.xueersi.kafkaserviceapi.constant.DelayConst.DELAY_RANK;
import static com.xueersi.kafkaserviceapi.constant.MessageParaConst.*;

@Log4j2
public class DelayMessageUtil {

    /**
     * 重新计算延时级别
     */
    public static Message recalculateDelayMessage(Message message) {
        if (!message.getHeaders().containsKey(MESSAGE_EXPIRE_TIME)) {
            return message;
        }
        int delayTime = (int) ((Long.parseLong(message.getHeaders().get(MESSAGE_EXPIRE_TIME).toString()) - System.currentTimeMillis()) / 1000);

        //计算延时级别
        Integer delayLevel = getDelayLevel(delayTime);
        //如果延时消息已经过期
        if (delayLevel == -1) {
            log.info("message:{}延时时间已经耗尽,剩余时间:{},将投入原topic", message.getMid(), delayTime);
            message.setTopic((String) message.getHeaders().get(ORIGIN_TOPIC));
            message.getHeaders().remove(EXPIRE_TIME_IN_TOPIC);
            message.getHeaders().remove(MESSAGE_EXPIRE_TIME);
            message.getHeaders().remove(ORIGIN_TOPIC);
        } else {
            log.info("延时消息message:{},剩余延时时间:{},将进入新的topic:{}", message.getMid(), delayTime, DELAY_PREFIX + delayLevel);
            message.getHeaders().put(EXPIRE_TIME_IN_TOPIC, System.currentTimeMillis() + delayLevel * 1000);
            message.setTopic(DELAY_PREFIX + delayLevel);
        }
        return message;
    }

    /**
     * 根据延时时间获取延时级别
     */
    public static Integer getDelayLevel(Integer delayTime) {
        if (delayTime <= 0) {
            return -1;
        }
        int timeRangeMin = 1;
        int timeRangeMax = 10;
        boolean findLevel = false;
        int delayLevel = -1;
        for (int i = 0; i < DELAY_RANK; i++) {
            if (delayTime >= timeRangeMin && delayTime < timeRangeMax) {
                //取整
                delayLevel = delayTime / timeRangeMin * timeRangeMin;
                findLevel = true;
                break;
            }
            timeRangeMin *= 10;
            timeRangeMax *= 10;
        }
        if (!findLevel) {
            if (delayTime >= Math.pow(10, DELAY_RANK)) {
                delayLevel = new Double(9 * Math.pow(10, DELAY_RANK - 1)).intValue();
            }
        }
        return delayLevel;
    }
}
