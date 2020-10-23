package com.xueersi.kafkaserviceapi.entity;

import lombok.Data;

import java.util.List;

@Data
public class BatchMessages {
    private List<Message> messages;
}
