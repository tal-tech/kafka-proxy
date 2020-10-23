package com.xueersi.kafkaserviceapi.entity;

import lombok.Builder;
import lombok.Data;


@Data
@Builder
public class CreateCompactTopicConfigs {
    private String remark;
    private String instantId;
    private String kafkaServers;
    private String topic;
    private Integer partitionNum;
    private Short replicationNum;
    private String cleanUpPolicy;
    private String minCleanAbleDirtyRatio;
    private String segmentMs;
    private String minCompactionLagMs;
    private String maxCompactionLagMs;

}
