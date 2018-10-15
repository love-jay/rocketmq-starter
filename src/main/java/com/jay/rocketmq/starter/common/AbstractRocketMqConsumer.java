package com.jay.rocketmq.starter.common;


import com.alibaba.fastjson.JSON;
import com.jay.rocketmq.starter.anno.RocketListenerHandler;
import com.jay.rocketmq.starter.config.RocketMqProperties;
import com.jay.rocketmq.starter.constants.ConsumeMode;
import com.jay.rocketmq.starter.constants.RocketMqTopic;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

/**
 * @author jay
 */
@EnableConfigurationProperties(RocketMqProperties.class)
public abstract class AbstractRocketMqConsumer<Topic extends RocketMqTopic, Content> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected Class<Topic> topicClazz;

    protected Class<Content> contentClazz;

    private boolean isStarted;

    private Integer consumeThreadMin;

    private Integer consumeThreadMax;

    private ConsumeFromWhere consumeFromWhere;

    private int delayLevelWhenNextConsume = 0;

    private long suspendCurrentQueueTimeMillis = -1;

    private MessageModel messageModel;

    private ConsumeMode consumeMode;

    private DefaultMQPushConsumer consumer;

    private String consumerGroup;

    private Map<String, Set<String>> annotationsMap;

    private String topic;

    private Set<String> tags;

    private String messageSelector;

    private int consumeMsgMaxSize;

    private int reConsumerTimes = 16;

    /**
     * 设置topic 和 tags
     */
//    public abstract Map<String, Set<String>> subscribeTopicTags();
    public Map<String, Set<String>> subscribeTopicTags(Map<String, Set<String>> customerMap) {
        if (null != customerMap) {
            return customerMap;
        }
        return this.annotationsMap;
    }


    /**
     * 设置消费组
     */
//    public abstract String getConsumerGroup();
    public String getConsumerGroup() {
        return consumerGroup;
    }

    public abstract boolean consumeMsg(Content content, MessageExt msg);

    @PostConstruct
    @SuppressWarnings("unchecked")
    public void init() throws MQClientException {
        Class<? extends AbstractRocketMqConsumer> parentClazz = this.getClass();
        Type genType = parentClazz.getGenericSuperclass();
        Type[] types = ((ParameterizedType) genType).getActualTypeArguments();
        topicClazz = (Class<Topic>) types[0];
        contentClazz = (Class<Content>) types[1];
        RocketListenerHandler handler = parentClazz.getAnnotation(RocketListenerHandler.class);
        if (null == handler) {
            throw new IllegalArgumentException("消费者缺少RocketListenerHandler注解！！！");
        } else {
            reConsumerTimes = handler.reTryConsume();
            consumeMsgMaxSize = handler.consumeMsgMaxSize();
            messageModel = handler.type();
            consumeFromWhere = handler.consumerFromWhere();
            consumeThreadMin = handler.consumeThreadMin();
            consumeThreadMax = handler.consumeThreadMax();
            consumeMode = handler.consumeMode();
            consumerGroup = handler.consumerGroup();
            messageSelector = handler.messageSelector();
            topic = handler.topic();
            String[] tagsArray = handler.tags();
            tags = new HashSet<>(Arrays.asList(tagsArray));
            if (!handler.reWriteSubscribe()) {
                annotationsMap = new HashMap<>();
                annotationsMap.put(topic, tags);
            }
        }
        if (this.isStarted()) {
            throw new IllegalStateException("Container already Started. " + this.toString());
        }
        initRocketMQPushConsumer();
    }

    @PreDestroy
    public void destroy() {
        if (Objects.nonNull(consumer)) {
            consumer.shutdown();
        }
        logger.info("consumer shutdown, {}", this.toString());
    }

    public class DefaultMessageListenerConcurrently implements MessageListenerConcurrently {

        @Override
        @SuppressWarnings("unchecked")
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            for (MessageExt messageExt : msgs) {
                try {
                    if (messageExt.getReconsumeTimes() > reConsumerTimes) {
                        logger.info("Message achieve reTryTimes, delivery to DLQ queue {},{},{},{}", reConsumerTimes, messageExt.getMsgId(), messageExt.getTopic(), messageExt.getTags());
                    }
                    if (consumeMsg(parseMsg(messageExt.getBody(), contentClazz), messageExt)) {
                        logger.info("Consume message: {},{},{}", messageExt.getMsgId(), messageExt.getTopic(), messageExt.getTags());
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    } else {
                        logger.info("Reject message:{},{},{}", messageExt.getMsgId(), messageExt.getTopic(), messageExt.getTags());
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                } catch (Exception e) {
                    logger.warn("Consume message failed. messageExt:{},{},{},{}", messageExt.getMsgId(), messageExt.getTopic(), messageExt.getTags(), e.getMessage());
                    context.setDelayLevelWhenNextConsume(delayLevelWhenNextConsume);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

    public class DefaultMessageListenerOrderly implements MessageListenerOrderly {

        @Override
        @SuppressWarnings("unchecked")
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            for (MessageExt messageExt : msgs) {
                try {
                    if (consumeMsg(parseMsg(messageExt.getBody(), contentClazz), messageExt)) {
                        logger.debug("Consume message: {}", messageExt.getMsgId());
                        return ConsumeOrderlyStatus.SUCCESS;
                    } else {
                        logger.info("Reject message:{}", messageExt.getMsgId());
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                } catch (Exception e) {
                    logger.warn("Consume message failed. messageExt:{}", messageExt, e);
                    context.setSuspendCurrentQueueTimeMillis(suspendCurrentQueueTimeMillis);
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
            }

            return ConsumeOrderlyStatus.SUCCESS;
        }
    }

    private void initRocketMQPushConsumer() throws MQClientException {

        Assert.notNull(getConsumerGroup(), "Property 'consumerGroup' is required");
        Assert.notEmpty(subscribeTopicTags(null), "SubscribeTopicTags method can't be empty");
        consumer = new DefaultMQPushConsumer(getConsumerGroup());
        consumer.setConsumeMessageBatchMaxSize(consumeMsgMaxSize);
        consumer.setMaxReconsumeTimes(reConsumerTimes <= 0 ? -1 : reConsumerTimes);
        if (StringUtils.hasText(messageSelector)) {
            consumer.subscribe(topic, MessageSelector.bySql(messageSelector));
        }
        if (consumeThreadMax != null) {
            consumer.setConsumeThreadMax(consumeThreadMax);
        }
        if (consumeThreadMax != null && consumeThreadMax < consumer.getConsumeThreadMin()) {
            consumer.setConsumeThreadMin(consumeThreadMax);
        }
        consumer.setConsumeFromWhere(consumeFromWhere);
        consumer.setMessageModel(messageModel);

        switch (consumeMode) {
            case Orderly:
//                consumer.setMessageListener(new DefaultMessageListenerOrderly());
                consumer.registerMessageListener(new DefaultMessageListenerOrderly());
                break;
            case CONCURRENTLY:
//                consumer.setMessageListener(new DefaultMessageListenerConcurrently());
                consumer.registerMessageListener(new DefaultMessageListenerConcurrently());
                break;
            default:
                throw new IllegalArgumentException("Property 'consumeMode' was wrong.");
        }

    }

    private <T> T parseMsg(byte[] body, Class<T> clazz) {
        T t = null;
        if (body != null) {
            try {
                t = JSON.parseObject(body, clazz);
            } catch (Exception e) {
                logger.error("Can not parse to Object", e);
            }
        }
        return t;
    }

    public Integer getConsumeThreadMin() {
        return consumeThreadMin;
    }

    public void setConsumeThreadMin(Integer consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }

    public Integer getConsumeThreadMax() {
        return consumeThreadMax;
    }

    public void setConsumeThreadMax(Integer consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }

    public boolean isStarted() {
        return isStarted;
    }

    public void setStarted(boolean started) {
        isStarted = started;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public int getDelayLevelWhenNextConsume() {
        return delayLevelWhenNextConsume;
    }

    public void setDelayLevelWhenNextConsume(int delayLevelWhenNextConsume) {
        this.delayLevelWhenNextConsume = delayLevelWhenNextConsume;
    }

    public long getSuspendCurrentQueueTimeMillis() {
        return suspendCurrentQueueTimeMillis;
    }

    public void setSuspendCurrentQueueTimeMillis(long suspendCurrentQueueTimeMillis) {
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public ConsumeMode getConsumeMode() {
        return consumeMode;
    }

    public void setConsumeMode(ConsumeMode consumeMode) {
        this.consumeMode = consumeMode;
    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }
}
