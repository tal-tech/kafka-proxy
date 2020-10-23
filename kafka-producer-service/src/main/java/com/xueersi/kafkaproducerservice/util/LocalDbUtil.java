package com.xueersi.kafkaproducerservice.util;

import com.google.gson.Gson;
import com.xueersi.kafkaproducerservice.config.mapdb.LocalDbConfig;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.ConcurrentMap;

@Data
@Log4j2
@Component
public class LocalDbUtil {
    @Autowired
    private LocalDbConfig localDbConfig;

    private DB db;
    private ConcurrentMap map;

    private Gson gson = new Gson();

    @PostConstruct
    public void init() {
        db = DBMaker
                .fileDB(localDbConfig.getConfig().get("path") + "/" + localDbConfig.getConfig().get("file"))
                .checksumHeaderBypass()
                .fileMmapEnable()
                .closeOnJvmShutdown()
                .make();

        map = db.hashMap(localDbConfig.getConfig().get("name")).createOrOpen();
    }

}
