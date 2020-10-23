package com.xueersi.kafkaserviceapi.constant;

public class MicrometerConst {
    public static final String COUNTER_SYMBOL = "Counter";
    public static final String GAUGE_SYMBOL = "Gauge";
    public static final String TIMER_SYMBOL = "Timer";
    public static final String SUMMARY_SYMBOL = "Summary";
    public static final String ATOMIC_LONG = "AtomicLong_";
    /**
     * 异步发送错误
     */
    public static final String PRODUCER_ASY_ERR = "producer_asy_err";
    /**
     * topic不存在的错误
     */
    public static final String TOPIC_NOT_EXIST_ERR = "topic_not_exist_err";
    /**
     * topic发送消息总数
     */
    public static final String TOPIC_SEND_NUM = "topic_send_num";
    /**
     * IP发送消息
     */
    public static final String IP_SEND_MSG = "producer_send_msg";
    /**
     * 提交消息错误统计
     */
    public static final String COMMIT_MSG_ERR = "commit_msg_err";
    /**
     * 提交消息错误统计
     */
    public static final String MSG_NUM_IN_MEM = "msg_num_in_mem";
}
