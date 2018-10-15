package com.jay.rocketmq.starter.constants;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.io.Serializable;

/**
 * @author jay
 */
public class RocketMqContent implements Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return JSON.toJSONString(this, SerializerFeature.NotWriteDefaultValue);
    }
}
