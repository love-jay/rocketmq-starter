package com.jay.rocketmq.starter.common;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * @author jay
 */
public class DefaultRocketMqProducer {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private DefaultMQProducer producer;

    /**
     * 发送消息
     */
    public boolean sendMessage(Message msg) {
        SendResult sendResult = null;
        try {
            sendResult = producer.send(msg);
        } catch (Exception e) {
            logger.error("send message error", e);
        }
        return result(sendResult);
    }


    public void sendMessage(Message msg, SendCallback sendCallback) {
        try {
            producer.send(msg, sendCallback);
        } catch (Exception e) {
            logger.error("send message error", e);
        }
    }

    public boolean sendMessage(List<Message> msg, long timeout) {
        SendResult sendResult = null;
        try {
            sendResult = producer.send(msg, timeout);
        } catch (Exception e) {
            logger.error("send message error", e);
        }
        return result(sendResult);
    }

    public boolean sendMessage(List<Message> msg) {
        SendResult sendResult = null;
        try {
            sendResult = producer.send(msg);
        } catch (Exception e) {
            logger.error("send message error", e);
        }
        return result(sendResult);
    }

    public boolean sendMessage(Message msg, long timeout) {
        SendResult sendResult = null;
        try {
            sendResult = producer.send(msg, timeout);
        } catch (Exception e) {
            logger.error("send message error", e);
        }
        return result(sendResult);
    }

    private boolean result(SendResult sendResult){
        boolean result = sendResult != null && sendResult.getSendStatus() == SendStatus.SEND_OK;
        if (!result) {
            logger.info("消息投递失败");
        } else {
            logger.info("消息：{} 投递成功", sendResult.getMsgId());
        }
        return result;
    }

    public DefaultMQProducer getProducer() {
        return producer;
    }

    public void setProducer(DefaultMQProducer producer) {
        this.producer = producer;
    }

    public void destroy() {
        if (Objects.nonNull(producer)) {
            producer.shutdown();
        }
    }
}
