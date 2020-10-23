package com.xueersi.kafkaserviceapi.util;

import com.xueersi.kafkaserviceapi.entity.CreateCompactTopicConfigs;
import com.xueersi.kafkaserviceapi.entity.CreateNormalTopicConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class SelfBuildTopicUtils {

    public static void createNormalTopic(CreateNormalTopicConfigs createNormalTopicConfigs) throws ExecutionException, InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, createNormalTopicConfigs.getKafkaServers());
        try (AdminClient adminClient = AdminClient.create(config)) {
            createNewTopic(adminClient, createNormalTopicConfigs.getTopic(), createNormalTopicConfigs.getPartitionNum(), createNormalTopicConfigs.getReplicationNum(), null);
        }
    }

    public static void createCompactTopic(CreateCompactTopicConfigs createCompactTopicConfigs) throws ExecutionException, InterruptedException {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, createCompactTopicConfigs.getKafkaServers());
        try (AdminClient adminClient = AdminClient.create(config)) {
            Map<String, String> configs = new HashMap<>();

            configs.put("cleanup.policy", createCompactTopicConfigs.getCleanUpPolicy());
            configs.put("min.cleanable.dirty.ratio", createCompactTopicConfigs.getMinCleanAbleDirtyRatio());
            configs.put("segment.ms", createCompactTopicConfigs.getSegmentMs());
            configs.put("min.compaction.lag.ms", createCompactTopicConfigs.getMinCompactionLagMs());
            configs.put("max.compaction.lag.ms", createCompactTopicConfigs.getMaxCompactionLagMs());

            createNewTopic(adminClient, createCompactTopicConfigs.getTopic(), createCompactTopicConfigs.getPartitionNum(), createCompactTopicConfigs.getReplicationNum(), configs);
        }
    }

    private static void createNewTopic(AdminClient adminClient, String topic, int partitions, short replication, Map<String, String> configs) throws ExecutionException, InterruptedException {

        List<NewTopic> newTopics = new ArrayList<>();

        NewTopic newTopic = new NewTopic(topic, partitions, replication);
        if (configs != null) {
            newTopic.configs(configs);
        }
        newTopics.add(newTopic);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);
        createTopicsResult.all().get();
    }

}
