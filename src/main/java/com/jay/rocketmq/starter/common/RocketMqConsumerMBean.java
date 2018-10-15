package com.jay.rocketmq.starter.common;

import java.util.List;

/**
 * @author hqr
 * @date 2018-08-10 09:28:42
 */
public class RocketMqConsumerMBean{

    private List<AbstractRocketMqConsumer> consumers;

    public List<AbstractRocketMqConsumer> getConsumers() {
        return consumers;
    }

    public void setConsumers(List<AbstractRocketMqConsumer> consumers) {
        this.consumers = consumers;
    }

}
