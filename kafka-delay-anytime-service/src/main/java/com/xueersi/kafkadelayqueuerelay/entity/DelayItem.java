package com.xueersi.kafkadelayqueuerelay.entity;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * 类说明：存放到队列的元素
 */

@Data
public class DelayItem implements Delayed {
    //到期时间，单位毫秒
    private long activeTime;
    private ConsumerRecord<String, String> consumerRecord;
    private TopicPartition topicPartition;
    private long offset;

    public DelayItem(long activeTime, ConsumerRecord<String, String> consumerRecord, TopicPartition topicPartition, long offset) {
        super();
        //将传入的时长转换为超时的时刻
        this.activeTime = activeTime;
        this.consumerRecord = consumerRecord;
        this.topicPartition = topicPartition;
        this.offset = offset;
    }

    //按照剩余时间排序
    @Override
    public int compareTo(@NotNull Delayed o) {
        DelayItem intemVo = (DelayItem) o;
        long d = getDelay(TimeUnit.MILLISECONDS) - intemVo.getDelay(TimeUnit.MILLISECONDS);
        if (d != 0) {
            return (d > 0) ? 1 : -1;
        } else if (this.topicPartition.partition() != intemVo.topicPartition.partition()) {
            return (this.topicPartition.partition() - intemVo.topicPartition.partition() > 0) ? 1 : -1;
        }
        return (this.offset - intemVo.offset > 0) ? 1 : -1;
    }

    //返回元素的剩余时间
    @Override
    public long getDelay(@NotNull TimeUnit unit) {
        return this.activeTime - System.currentTimeMillis();
    }
}
