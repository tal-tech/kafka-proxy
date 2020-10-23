package com.xueersi.kafkaproducerservice.controller;

import com.xueersi.kafkaproducerservice.micrometer.MicroMeter;
import com.xueersi.kafkaproducerservice.service.MsgHandlerService;
import com.xueersi.kafkaproducerservice.service.MsgSendService;
import com.xueersi.kafkaproducerservice.util.LocalDbUtil;
import com.xueersi.kafkaserviceapi.entity.BatchMessages;
import com.xueersi.kafkaserviceapi.entity.Message;
import com.xueersi.kafkaserviceapi.entity.ReturnData;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

import static com.xueersi.kafkaproducerservice.service.MsgHandlerService.SERVICE_STOP;
import static com.xueersi.kafkaserviceapi.Enum.ReturnCodeEnum.*;
import static com.xueersi.kafkaserviceapi.util.AddrOperator.getIpAddr;

@Api("kafka生产者")
@Log4j2
@RestController
@RequestMapping("/v1/kafka")
public class ProducerController {
    @Autowired
    private MsgHandlerService msgHandlerService;
    @Autowired
    private MicroMeter microMeter;
    @Autowired
    private LocalDbUtil localDbUtil;
    @Autowired
    private MsgSendService msgSendService;
    @Value("${proxy.config.auto-create-topic}")
    private Boolean autoCreateTopic;

    @ApiOperation(value = "发送消息")
    @ResponseBody
    @RequestMapping(value = "/send", method = RequestMethod.POST)
    public ReturnData kafkaSendMsg(@RequestBody Message message) {
        ArrayList<Message> failList = new ArrayList<>();
        if (SERVICE_STOP.get()) {
            failList.add(message);
            return new ReturnData(SEND_FAILED.getCode(), failList, SEND_FAILED.getReturnMsg());
        }
        String topic = message.getTopic();
        //如果不是测试环境,则检查topic是否创建
        if (!autoCreateTopic && msgHandlerService.isTopicNotCreated(topic)) {
            microMeter.getTopicNotExistErrorCounter(topic).increment();
            return new ReturnData(TOPIC_NOT_EXIST.getCode(), failList, TOPIC_NOT_EXIST.getReturnMsg());
        }
        try {
            //延时消息处理
            msgHandlerService.checkDelayMessage(message);
        } catch (RuntimeException e) {
            log.error("延时消息解析有误{}", message, e);
            return new ReturnData(DELAY_LEVEL_ERROR.getCode(), null, DELAY_LEVEL_ERROR.getReturnMsg());
        }
        String ip = getIpAddr();
        if (!"".equals(ip)) {
            message.setProducerIP(ip);
            microMeter.getIpSendMsgCounter(ip, topic).increment();
        }
        msgSendService.sendMsg(message);

        return new ReturnData(SEND_SUCCESS.getCode(), null, SEND_SUCCESS.getReturnMsg());
    }


    @RequestMapping(value = "/send_batch", method = RequestMethod.POST)
    public ReturnData sendBatch(@RequestBody BatchMessages messages) {
        List<Message> failList = new ArrayList<>();
        if (SERVICE_STOP.get()) {
            failList = messages.getMessages();
            return new ReturnData(BATCH_SEND_FAILED.getCode(), failList, BATCH_SEND_FAILED.getReturnMsg());
        }

        String ip = getIpAddr();
        List<Message> messagesList = messages.getMessages();
        for (Message message : messagesList) {
            String topic = message.getTopic();
            if (!autoCreateTopic && msgHandlerService.isTopicNotCreated(topic)) {
                failList.add(message);
                microMeter.getTopicNotExistErrorCounter(topic).increment();
            }
            try {
                msgHandlerService.checkDelayMessage(message);
            } catch (Exception e) {
                log.error("延时消息解析有误{}", message, e);
                failList.add(message);
            }
            if (!"".equals(ip)) {
                message.setProducerIP(ip);
                microMeter.getIpSendMsgCounter(ip, topic).increment();
            }
            msgSendService.sendMsg(message);
        }
        if (failList.size() > 0) {
            return new ReturnData(BATCH_SEND_FAILED.getCode(), failList, BATCH_SEND_FAILED.getReturnMsg());
        }
        return new ReturnData(BATCH_SEND_SUCCESS.getCode(), null, BATCH_SEND_SUCCESS.getReturnMsg());
    }

    @RequestMapping(value = "/clearDb", method = RequestMethod.GET)
    public ReturnData clearDb() {
        SERVICE_STOP.set(true);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (localDbUtil.getMap().size() > 0) {
            localDbUtil.getMap().keySet().forEach(key -> {
                log.warn("---> check db: " + localDbUtil.getMap().get(key));
                localDbUtil.getMap().remove(key);
            });
        }

        SERVICE_STOP.set(false);
        return new ReturnData(CLEAR_DB_SUCCESS.getCode(), null, CLEAR_DB_SUCCESS.getReturnMsg());
    }

    @RequestMapping(value = "/stopService", method = RequestMethod.GET)
    public ReturnData stopService() {
        SERVICE_STOP.set(true);
        return new ReturnData(BIZ_STOP_SUCCESS.getCode(), null, BIZ_STOP_SUCCESS.getReturnMsg());
    }

    @RequestMapping(value = "/resumeService", method = RequestMethod.GET)
    public ReturnData resumeService() {
        SERVICE_STOP.set(false);
        return new ReturnData(BIZ_RESUME_SUCCESS.getCode(), null, BIZ_RESUME_SUCCESS.getReturnMsg());
    }

    @RequestMapping(value = "/getServiceStatus", method = RequestMethod.GET)
    public ReturnData getServiceStatus() {
        return new ReturnData(PEEK_BIZ_STATUS_SUCCESS.getCode(), "SERVICE_STOP:" + SERVICE_STOP.get()
                , PEEK_BIZ_STATUS_SUCCESS.getReturnMsg());
    }
}
