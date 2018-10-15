package com.jay.rocketmq.starter.anno;


import com.jay.rocketmq.starter.constants.ConsumeMode;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author jay
 * @version 1.0
 * @className RocketListenerHandler
 * @date 2018/08/31 10:45
 * @description 自定义RocketMq consumer注解
 * @program framework-rocketmq-starter
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface RocketListenerHandler {

    MessageModel type() default MessageModel.CLUSTERING;

    ConsumeFromWhere consumerFromWhere() default ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    int consumeThreadMin() default 1;

    int consumeThreadMax() default 1;

    ConsumeMode consumeMode() default ConsumeMode.CONCURRENTLY;

    int consumeMsgMaxSize() default 1;

    int reTryConsume() default 16;

    String consumerGroup();

    String topic();

    String[] tags();

    String messageSelector() default "";

    /**
     * 如果 为true，那么必须复写subscribeTopicTags方法，同时配置的topic 和tags失效
     * @return
     */
    boolean reWriteSubscribe() default false;
}
