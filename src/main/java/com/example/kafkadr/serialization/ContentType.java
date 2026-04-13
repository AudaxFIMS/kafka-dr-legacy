package com.example.kafkadr.serialization;

/**
 * Supported content types for consumer/producer serialization.
 */
public enum ContentType {
    STRING,
    JSON,
    BYTES,
    NATIVE;

    public static ContentType fromString(String value) {
        if (value == null) return STRING;
        switch (value.toLowerCase()) {
            case "json": return JSON;
            case "bytes": return BYTES;
            case "native": return NATIVE;
            case "string":
            default: return STRING;
        }
    }
}
