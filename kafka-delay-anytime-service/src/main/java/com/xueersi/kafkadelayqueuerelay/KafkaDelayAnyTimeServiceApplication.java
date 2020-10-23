package com.xueersi.kafkadelayqueuerelay;

import com.xueersi.kafkadelayqueuerelay.init.InitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletListenerRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class KafkaDelayAnyTimeServiceApplication {
    @Autowired
    private InitListener initListener;

    public static void main(String[] args) {
        SpringApplication.run(KafkaDelayAnyTimeServiceApplication.class, args);
    }

    @Bean
    public ServletListenerRegistrationBean<InitListener> servletListenerRegistrationBean() {
        ServletListenerRegistrationBean<InitListener> servletListenerRegistrationBean = new ServletListenerRegistrationBean<>();
        servletListenerRegistrationBean.setListener(initListener);
        return servletListenerRegistrationBean;
    }

}
