package com.xueersi.kafkaserviceapi.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
public class CreateNormalTopicConfigs {
    private String remark;
    private String instantId;
    private String kafkaServers;
    private String topic;
    private Integer partitionNum;
    private Short replicationNum;
}
