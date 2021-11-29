package com.fullonnet.deserializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Deserializes a Generic Stream.
 *
 * @param <T>
 */
public class CustomValueDeserializer<T> implements Deserializer<T> {

    private static final Logger LOGGER = Logger.getLogger(CustomValueDeserializer.class.getName());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public CustomValueDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        LOGGER.debug("Configuring CustomValueDeserializer...");
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        LOGGER.debug("Topic: " + topic + " bytes: " + new String(bytes, StandardCharsets.UTF_8));
        try {
            InputStream myInputStream = new ByteArrayInputStream(bytes);
            Map<String, Object> map = OBJECT_MAPPER.readValue(myInputStream, new TypeReference<Map<String, Object>>() {
            });
            map.put("my-property", "1234");
            LOGGER.debug(map);
            return (T) OBJECT_MAPPER.writeValueAsString(map);
        } catch (Exception e) {
            System.err.println(e);
            return null;
        }
    }
}
