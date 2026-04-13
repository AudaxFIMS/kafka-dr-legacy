package com.example.kafkadr.config;

import java.util.LinkedHashMap;
import java.util.Map;

public class ProducerConfig {

    private String topic;
    private String contentType;
    private Map<String, Object> properties = new LinkedHashMap<>();

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
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
        return "ProducerConfig{topic='" + topic + "', contentType='" + contentType + "'}";
    }
}
