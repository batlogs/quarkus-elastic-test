package org.acme.service;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.otaibe.commons.quarkus.elasticsearch.client.service.AbstractElasticsearchService;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;

@ApplicationScoped
@Getter
@Setter
@Slf4j
public class ElasticService extends AbstractElasticsearchService {

    public void init(@Observes StartupEvent event) {
        log.info("init started");
        super.init();
        log.info("init completed");
    }

    public void shutdown(@Observes ShutdownEvent event) {
        log.info("shutdown started");
        super.shutdown();
        log.info("shutdown completed");
    }
}
