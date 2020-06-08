package org.acme.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CustomObjectMapperConfig {

    public CustomObjectMapperConfig() {
    }

    public void fillObjectMapper(ObjectMapper objectMapper1) {
        objectMapper1.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper1.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }
}
