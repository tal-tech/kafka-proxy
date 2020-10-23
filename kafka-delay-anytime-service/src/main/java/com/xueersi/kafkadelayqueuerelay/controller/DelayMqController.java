package com.xueersi.kafkadelayqueuerelay.controller;

import com.xueersi.kafkaserviceapi.entity.ReturnData;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import static com.xueersi.kafkadelayqueuerelay.service.KafkaDelayQService.SERVICE_STOP;

public class DelayMqController {
    @ResponseBody
    @RequestMapping(value = "/shutdown", method = RequestMethod.GET)
    public ReturnData backupUncommitMsg() {
        //业务暂停
        SERVICE_STOP.set(true);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(0);
        }
        return new ReturnData(0, null, "关闭成功");
    }
}
