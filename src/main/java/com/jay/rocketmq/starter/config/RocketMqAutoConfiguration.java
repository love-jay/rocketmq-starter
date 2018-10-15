package com.jay.rocketmq.starter.config;


import com.jay.rocketmq.starter.common.AbstractRocketMqConsumer;
import com.jay.rocketmq.starter.common.DefaultRocketMqProducer;
import com.jay.rocketmq.starter.common.RocketMqConsumerMBean;
import com.jay.rocketmq.starter.util.RunTimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author jay
 */
@Configuration
@ConditionalOnClass({ DefaultMQPushConsumer.class })
@EnableConfigurationProperties(RocketMqProperties.class)
public class RocketMqAutoConfiguration {

    private final static Logger LOGGER = LoggerFactory.getLogger(RocketMqAutoConfiguration.class);

    @Resource
    private RocketMqProperties rocketMqProperties;

    @Bean
    @ConditionalOnClass(DefaultMQProducer.class)
    @ConditionalOnMissingBean(DefaultMQProducer.class)
    public DefaultMQProducer mqProducer() {
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setProducerGroup(rocketMqProperties.getProducerGroupName());
        producer.setNamesrvAddr(rocketMqProperties.getNamesrvAddr());

        producer.setSendMsgTimeout(rocketMqProperties.getSendMsgTimeout());
        producer.setRetryTimesWhenSendFailed(rocketMqProperties.getRetryTimesWhenSendFailed());
        producer.setRetryTimesWhenSendAsyncFailed(rocketMqProperties.getRetryTimesWhenSendAsyncFailed());
        producer.setMaxMessageSize(rocketMqProperties.getMaxMessageSize());
        producer.setCompressMsgBodyOverHowmuch(rocketMqProperties.getCompressMsgBodyOverHowMuch());
        producer.setRetryAnotherBrokerWhenNotStoreOK(rocketMqProperties.isRetryAnotherBrokerWhenNotStoreOk());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Producer shutdown");
            producer.shutdown();
        }));

        try {
            producer.start();
            LOGGER.info("RocketMQ Producer Started, NamesrvAddr:{}, Group:{}", rocketMqProperties.getNamesrvAddr(),
                    rocketMqProperties.getProducerGroupName());
        } catch (MQClientException e) {
            LOGGER.error("Producer Start ERROR, NamesrvAddr:{}, Group:{}", rocketMqProperties.getNamesrvAddr(),
                    rocketMqProperties.getProducerGroupName(), e);
        }

        return producer;
    }

    @Bean(destroyMethod = "destroy")
    @ConditionalOnBean(DefaultMQProducer.class)
    @ConditionalOnMissingBean(name = "defaultRocketMqProducer")
    public DefaultRocketMqProducer defaultRocketMqProducer(@Qualifier("mqProducer") DefaultMQProducer mqProducer) {
        DefaultRocketMqProducer defaultRocketMqProducer = new DefaultRocketMqProducer();
        defaultRocketMqProducer.setProducer(mqProducer);
        return defaultRocketMqProducer;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(value = AbstractRocketMqConsumer.class)
    @Order
    public RocketMqConsumerMBean rocketMqConsumerMBean(List<AbstractRocketMqConsumer> messageListeners) {
        RocketMqConsumerMBean rocketMqConsumerMBean = new RocketMqConsumerMBean();
        messageListeners.forEach(this::registerMQConsumer);
        rocketMqConsumerMBean.setConsumers(messageListeners);
        return rocketMqConsumerMBean;
    }

    @SuppressWarnings("unchecked")
    private void registerMQConsumer(AbstractRocketMqConsumer rocketMqConsumer) {
        Map<String, Set<String>> subscribeTopicTags = rocketMqConsumer.subscribeTopicTags(null);
        DefaultMQPushConsumer mqPushConsumer = rocketMqConsumer.getConsumer();
        // 必须为每个mqPushConsumer设置InstanceName
        mqPushConsumer.setInstanceName(RunTimeUtil.getRocketMqUniqeInstanceName());
        mqPushConsumer.setNamesrvAddr(rocketMqProperties.getNamesrvAddr());
        subscribeTopicTags.entrySet().forEach(e -> {
            try {
                String rocketMqTopic = e.getKey();
                Set<String> rocketMqTags = e.getValue();
                if (CollectionUtils.isEmpty(rocketMqTags)) {
                    mqPushConsumer.subscribe(rocketMqTopic, "*");
                } else {
                    String tags = StringUtils.join(rocketMqTags, " || ");
                    mqPushConsumer.subscribe(rocketMqTopic, tags);
                    LOGGER.info("Subscribe, Topic:{}, Tags:{}", rocketMqTopic, tags);
                }
            } catch (MQClientException ex) {
                LOGGER.error("Consumer Subscribe error", ex);
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Consumer shutdown");
            mqPushConsumer.shutdown();
        }));

        try {
            mqPushConsumer.start();
            rocketMqConsumer.setStarted(true);
            LOGGER.info("RocketMQ Consumer Started, NamesrvAddr:{}, Group:{}", rocketMqProperties.getNamesrvAddr(),
                    rocketMqConsumer.getConsumerGroup());
        } catch (MQClientException e) {
            LOGGER.error("Consumer start error, NamesrvAddr:{}, Group:{}", rocketMqProperties.getNamesrvAddr(),
                    rocketMqConsumer.getConsumerGroup(), e);
        }

    }

}
