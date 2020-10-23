package com.xueersi.kafkaserviceapi.entity;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

@Data
@ToString
public class Message implements Serializable {
    /**
     * 消息Id
     * ps:可以由用户自定义,也可以由proxy生成
     */
    private String mid;
    private String topic;
    /**
     * 消息体
     */
    private String payload;
    private String key;
    /**
     * 消息的头信息
     * ps:延时消息需要用到这个字段,如: "x-delay":5,即为延时等级为5秒的消息请求
     */
    private Map<String, Object> headers;
    /**
     * 消息的来源IP
     */
    private String producerIP;
}
