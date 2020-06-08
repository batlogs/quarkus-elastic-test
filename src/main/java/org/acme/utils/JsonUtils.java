package org.acme.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.acme.config.CustomObjectMapperConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@ApplicationScoped
public class JsonUtils {
    private static final Logger log = LoggerFactory.getLogger(JsonUtils.class);
    private static final Logger logger = LoggerFactory.getLogger(JsonUtils.class);
    @Inject
    ObjectMapper objectMapper;
    @Inject
    CustomObjectMapperConfig customObjectMapperConfig;

    public JsonUtils() {
    }

    @PostConstruct
    void init() {
    }

    public <T> T fromMap(Map input, Class<T> outputClass) {
        return this.fromMap(input, this.getObjectMapper(), outputClass);
    }

    public <T> T fromMap(Map input, ObjectMapper objectMapper, Class<T> outputClass) {
        try {
            T value = objectMapper.readValue(objectMapper.writeValueAsBytes(input), outputClass);
            return value;
        } catch (IOException var5) {
            logger.error("unable to transform to outputClass", var5);
            throw new RuntimeException(var5);
        }
    }

    public Map toMap(Object input) {
        return this.toMap(input, this.getObjectMapper());
    }

    public Map toMap(Object input, ObjectMapper objectMapper) {
        return this.toMap(input, objectMapper, new HashMap());
    }

    public Map<String, Object> toMap(Object input, Map<String, Function<Object, Object>> valueChangeMap) {
        return this.toMap(input, this.getObjectMapper(), valueChangeMap);
    }

    public Map<String, Object> toMap(Object input, ObjectMapper objectMapper, Map<String, Function<Object, Object>> valueChangeMap) {
        try {
            Map<String, Object> map = (Map)objectMapper.readValue(objectMapper.writeValueAsBytes(input), Map.class);
            return (Map)map.keySet().stream().filter((s) -> {
                return s != null;
            }).filter((s) -> {
                return null != map.get(s);
            }).filter((s) -> {
                return !map.get(s).toString().isEmpty() && 0 != map.get(s).toString().charAt(0);
            }).collect(Collectors.toMap((o) -> {
                return o;
            }, (o) -> {
                Object val = map.get(o);
                return valueChangeMap.containsKey(o) ? ((Function)valueChangeMap.get(o)).apply(val) : val;
            }));
        } catch (IOException var5) {
            logger.error("unable to transform to Map", var5);
            throw new RuntimeException(var5);
        }
    }

    public <T> T deepClone(Object input, Class<T> resultType) {
        return this.deepClone(input, this.getObjectMapper(), resultType);
    }

    public <T> T deepClone(Object input, ObjectMapper objectMapper, Class<T> resultType) {
        if (input == null) {
            return null;
        } else {
            try {
                return objectMapper.readValue(objectMapper.writeValueAsBytes(input), resultType);
            } catch (IOException var5) {
                logger.error("unable to serialize", var5);
                throw new RuntimeException(var5);
            }
        }
    }

    public Object toStringLazy(Object input) {
        return this.toStringLazy(input, this.getObjectMapper());
    }

    public Object toStringLazy(Object input, ObjectMapper objectMapper) {
        return new JsonUtils.ToStringLazy(input, objectMapper);
    }

    public <T> Optional<T> readValue(String value, Class<T> clazz) {
        return this.readValue(value, clazz, this.getObjectMapper());
    }

    public <T> Optional<T> readValue(byte[] value, Class<T> clazz) {
        return this.readValue(value, clazz, this.getObjectMapper());
    }

    public <T> Optional<T> readValue(InputStream value, Class<T> clazz) {
        return this.readValue(value, clazz, this.getObjectMapper());
    }

    public <T> Optional<T> readValue(String value, Class<T> clazz, ObjectMapper objectMapper1) {
        return Optional.ofNullable(objectMapper1).map((objectMapper) -> {
            try {
                return objectMapper.readValue(value, clazz);
            } catch (IOException var4) {
                logger.error("unable to deserialize", var4);
                return null;
            }
        });
    }

    public <T> Optional<T> readValue(byte[] value, Class<T> clazz, ObjectMapper objectMapper1) {
        return Optional.ofNullable(objectMapper1).map((objectMapper) -> {
            try {
                return objectMapper.readValue(value, clazz);
            } catch (IOException var4) {
                logger.error("unable to deserialize", var4);
                return null;
            }
        });
    }

    public <T> Optional<T> readValue(InputStream value, Class<T> clazz, ObjectMapper objectMapper1) {
        return Optional.ofNullable(objectMapper1).map((objectMapper) -> {
            try {
                return objectMapper.readValue(value, clazz);
            } catch (IOException var4) {
                logger.error("unable to deserialize", var4);
                return null;
            }
        });
    }

    public ObjectMapper getObjectMapper() {
        return this.objectMapper;
    }

    public CustomObjectMapperConfig getCustomObjectMapperConfig() {
        return this.customObjectMapperConfig;
    }

    private static class ToStringLazy {
        private Object input;
        private ObjectMapper objectMapper;

        public String toString() {
            if (this.objectMapper != null && this.input != null) {
                try {
                    String value = this.objectMapper.writeValueAsString(this.input);
                    return value;
                } catch (Exception var2) {
                    JsonUtils.logger.error("unable to serialize to json", var2);
                    return "";
                }
            } else {
                return "";
            }
        }

        public ToStringLazy(Object input, ObjectMapper objectMapper) {
            this.input = input;
            this.objectMapper = objectMapper;
        }
    }
}
