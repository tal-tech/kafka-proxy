package com.xueersi.kafkaserviceapi.constant;

public class MessageParaConst {
    /**
     * 生产者IP
     */
    public static final String PRODUCER_ID = "proxy_producer_ip";
    /**
     * 消息ID
     */
    public static final String MSG_ID = "proxy_msg_id";
    /**
     * 延时标记
     */
    public static final String X_DELAY = "x-delay";
    /**
     * 消息在该延时队列中的过期时间
     */
    public static final String EXPIRE_TIME_IN_TOPIC = "expireTimeInTopic";
    /**
     * 消息过期时间
     */
    public static final String MESSAGE_EXPIRE_TIME = "msgExpireTime";
    /**
     * 消息原始的topic
     */
    public static final String ORIGIN_TOPIC = "originTopic";
}
