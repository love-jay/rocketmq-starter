package com.jay.rocketmq.starter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author hqr
 * @date 2018-08-10 09:31:53
 */
@ConfigurationProperties(prefix = RocketMqProperties.PREFIX)
public class RocketMqProperties {

    static final String PREFIX = "spring.rocketmq";

    private String namesrvAddr;

    private String producerGroupName;

    private int sendMsgTimeout = 3000;

    private int compressMsgBodyOverHowMuch = 1024 * 4;

    private int retryTimesWhenSendFailed = 2;

    private int retryTimesWhenSendAsyncFailed = 2;

    private boolean retryAnotherBrokerWhenNotStoreOk = false;
    private int maxMessageSize = 1024 * 1024 * 4;


    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getProducerGroupName() {
        return producerGroupName;
    }

    public void setProducerGroupName(String producerGroupName) {
        this.producerGroupName = producerGroupName;
    }

    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public int getCompressMsgBodyOverHowMuch() {
        return compressMsgBodyOverHowMuch;
    }

    public void setCompressMsgBodyOverHowMuch(int compressMsgBodyOverHowMuch) {
        this.compressMsgBodyOverHowMuch = compressMsgBodyOverHowMuch;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public int getRetryTimesWhenSendAsyncFailed() {
        return retryTimesWhenSendAsyncFailed;
    }

    public void setRetryTimesWhenSendAsyncFailed(int retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }

    public boolean isRetryAnotherBrokerWhenNotStoreOk() {
        return retryAnotherBrokerWhenNotStoreOk;
    }

    public void setRetryAnotherBrokerWhenNotStoreOk(boolean retryAnotherBrokerWhenNotStoreOk) {
        this.retryAnotherBrokerWhenNotStoreOk = retryAnotherBrokerWhenNotStoreOk;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

}
