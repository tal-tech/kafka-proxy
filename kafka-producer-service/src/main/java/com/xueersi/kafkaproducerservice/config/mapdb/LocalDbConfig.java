package com.xueersi.kafkaproducerservice.config.mapdb;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "localdb")
public class LocalDbConfig {
    private Map<String, String> config;
}
