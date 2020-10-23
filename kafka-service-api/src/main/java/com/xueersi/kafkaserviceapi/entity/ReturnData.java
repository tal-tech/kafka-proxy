package com.xueersi.kafkaserviceapi.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class ReturnData implements Serializable {
    private int code;
    private Object data;
    private String msg;
}
