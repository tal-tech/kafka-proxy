package com.xueersi.kafkaproducerservice.shutdown;

import io.undertow.Undertow;
import io.undertow.server.ConnectorStatistics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.embedded.undertow.UndertowServletWebServer;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.List;

import static com.xueersi.kafkaproducerservice.service.MsgHandlerService.SERVICE_STOP;

@Component
public class GracefulShutdown implements ApplicationListener<ContextClosedEvent> {
    @Autowired
    private GracefulShutdownWrapper gracefulShutdownWrapper;
    @Autowired
    private ServletWebServerApplicationContext context;

    @Override
    public void onApplicationEvent(ContextClosedEvent contextClosedEvent) {
        gracefulShutdownWrapper.getGracefulShutdownHandler().shutdown();
        try {
            System.out.println("系统即将关闭...");
            SERVICE_STOP.set(true);
            UndertowServletWebServer webServer = (UndertowServletWebServer) context.getWebServer();
            Field field = webServer.getClass().getDeclaredField("undertow");
            field.setAccessible(true);
            Undertow undertow = (Undertow) field.get(webServer);
            List<Undertow.ListenerInfo> listenerInfo = undertow.getListenerInfo();
            Undertow.ListenerInfo listener = listenerInfo.get(0);
            ConnectorStatistics connectorStatistics = listener.getConnectorStatistics();
            if (connectorStatistics.getActiveConnections() > 0) {
                Thread.sleep(3000);
            }
        } catch (Exception e) {
            // Application Shutdown
        }
    }
}