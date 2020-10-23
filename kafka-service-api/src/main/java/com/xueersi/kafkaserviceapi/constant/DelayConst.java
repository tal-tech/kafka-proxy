package com.xueersi.kafkaserviceapi.constant;

public class DelayConst {
    /**
     * 延时队列名称前缀
     */
    public static final String DELAY_PREFIX = "mqp-delay-";
    /**
     * 创建内置延时队列的topic数, 总数为rank*9
     */
    public static final int DELAY_RANK = 4;
}
