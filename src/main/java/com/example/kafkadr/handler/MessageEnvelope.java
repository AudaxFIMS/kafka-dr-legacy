package com.example.kafkadr.handler;

import com.example.kafkadr.serialization.ContentType;

/**
 * Type-safe envelope wrapping a deserialized Kafka record.
 *
 * @param <T> the deserialized value type, matching the handler's generic parameter
 */
public class MessageEnvelope<T> {

    private final String topic;
    private final Object key;
    private final T value;
    private final int partition;
    private final long offset;
    private final String clusterName;
    private final ContentType contentType;

    public MessageEnvelope(String topic, Object key, T value,
                           int partition, long offset,
                           String clusterName, ContentType contentType) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.offset = offset;
        this.clusterName = clusterName;
        this.contentType = contentType;
    }

    public String getTopic() {
        return topic;
    }

    public Object getKey() {
        return key;
    }

    /** The deserialized value, typed as {@code T} */
    public T getValue() {
        return value;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public String getClusterName() {
        return clusterName;
    }

    public ContentType getContentType() {
        return contentType;
    }

    @Override
    public String toString() {
        return "MessageEnvelope{topic='" + topic + "', partition=" + partition +
                ", offset=" + offset + ", contentType=" + contentType +
                ", cluster='" + clusterName + "'}";
    }
}
