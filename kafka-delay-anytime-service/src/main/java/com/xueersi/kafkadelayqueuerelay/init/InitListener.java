package com.xueersi.kafkadelayqueuerelay.init;

import com.xueersi.kafkadelayqueuerelay.entity.DelayItem;
import com.xueersi.kafkadelayqueuerelay.service.KafkaDelayQService;
import com.xueersi.kafkaserviceapi.entity.CreateNormalTopicConfigs;
import com.xueersi.kafkaserviceapi.util.SelfBuildTopicUtils;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.DelayQueue;

import static com.xueersi.kafkaserviceapi.constant.DelayConst.DELAY_PREFIX;
import static com.xueersi.kafkaserviceapi.constant.DelayConst.DELAY_RANK;
import static com.xueersi.kafkaserviceapi.util.AddrOperator.getIpAddr;

@Data
@Log4j2
@Configuration
@EnableScheduling
public class InitListener implements ServletContextListener {

    public static String IP_PORT;
    @Autowired
    KafkaDelayQService kafkaDelayQService;
    @Value("${server.port}")
    private String port;
    @Value("${kafka.bootstrap-servers}")
    private String kafkaServers;
    @Value("${proxy.config.delay.partition-num}")
    private Integer partitionNum;
    @Value("${proxy.config.delay.replication}")
    private Short replication;

    private AdminClient adminClient;

    private void init() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        try {
            adminClient = AdminClient.create(props);
            initDelayTopic();
        } catch (Exception e) {
            log.error("系统启动初始化topic失败,系统退出!", e);
            System.exit(0);
        } finally {
            if (adminClient != null) {
                adminClient.close();
            }
        }
        IP_PORT = getIpAddr() + ":" + port;
        //创建 9个线程去消费内置的延时topic
        for (int i = 1; i < 10; i++) {
            DelayQueue<DelayItem> delayQueue = new DelayQueue<>();
            kafkaDelayQService.consumerMsg(DELAY_PREFIX + i, delayQueue, IP_PORT);
        }
    }

    /**
     * 创建内置的延时队列
     */
    public void initDelayTopic() throws Exception {
        Set<String> topicToCreatSet = new HashSet<>();
        //延时级别
        for (int i = 0; i < DELAY_RANK; i++) {
            for (int j = 1; j <= 9; j++) {
                int delayLevel = j * new Double(Math.pow(10, i)).intValue();
                String topic = DELAY_PREFIX + delayLevel;
                topicToCreatSet.add(topic);
            }
        }

        Set<String> topicExistSet = topicLists();
        log.info("延时相关的topic:{}", topicToCreatSet);
        topicToCreatSet.removeAll(topicExistSet);
        log.info("待创建的topic:{}", topicToCreatSet);
        if (topicToCreatSet.isEmpty()) {
            return;
        }
        for (String topic : topicToCreatSet) {
            CreateNormalTopicConfigs createNormalTopicConfigs = getNormalTopicConfig(topic);
            SelfBuildTopicUtils.createNormalTopic(createNormalTopicConfigs);
        }
    }

    public Set<String> topicLists() throws Exception {
        //是否查看internal选项,internal,就是kafka内置的topic
        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        //有传入参数的方法,通过参数控制是否显示kafka内置topic
        listTopicsOptions.listInternal(false);
        ListTopicsResult listTopicsResult = adminClient.listTopics(listTopicsOptions);
        return listTopicsResult.names().get();
    }

    private CreateNormalTopicConfigs getNormalTopicConfig(String topic) {
        log.info("创建延时队列:{}", topic);
        return CreateNormalTopicConfigs.builder()
                .topic(topic)
                .kafkaServers(kafkaServers)
                .partitionNum(partitionNum)
                .replicationNum(replication)
                .remark("创建延时队列:" + topic)
                .build();
    }

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        init();
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {

    }

}
