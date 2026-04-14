package com.example.kafkadr.config;

import java.util.LinkedHashMap;
import java.util.Map;

public class DrConsumerConfig {

    private String topic;
    private String group;
    private String handler;
    private String keyContentType = "string";
    private String contentType;
    private Map<String, Object> properties = new LinkedHashMap<>();

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getHandler() {
        return handler;
    }

    public void setHandler(String handler) {
        this.handler = handler;
    }

    public String getKeyContentType() {
        return keyContentType;
    }

    public void setKeyContentType(String keyContentType) {
        this.keyContentType = keyContentType;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "ConsumerConfig{topic='" + topic + "', group='" + group +
                "', handler='" + handler + "', contentType='" + contentType + "'}";
    }
}
