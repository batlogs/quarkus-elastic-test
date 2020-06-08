package org.acme.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


public abstract class AbstractElasticsearchService {
    private static final Logger log = LoggerFactory.getLogger(AbstractElasticsearchService.class);

    @ConfigProperty(name = "service.elastic-search.hosts", defaultValue = "0.0.0.0:9200")
    String[] hosts;

    @ConfigProperty(name = "service.elastic-search.num-threads", defaultValue = "10")
    Optional<Integer> numThreads;

    private RestHighLevelClient restClient;
    private Sniffer sniffer;

    public AbstractElasticsearchService() {
    }

    @PostConstruct
    public void init() {
        log.info("init started");
        List<HttpHost> httpHosts = (List) Arrays.stream(this.hosts).map((s) -> {
            return StringUtils.split(s, ':');
        }).map((strings) -> {
            return new HttpHost(strings[0], Integer.valueOf(strings[1]));
        }).collect(Collectors.toList());
        RestClientBuilder builder = RestClient.builder((HttpHost[])httpHosts.toArray(new HttpHost[httpHosts.size()]));
        this.getNumThreads().ifPresent((integer) -> {
            builder.setHttpClientConfigCallback((httpClientBuilder) -> {
                return httpClientBuilder.setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(integer).build());
            });
        });
        this.restClient = new RestHighLevelClient(builder);
        this.sniffer = Sniffer.builder(this.getRestClient().getLowLevelClient()).build();
        log.info("init completed");
    }

    public void shutdown() {
        log.info("shutdown started");
        this.getSniffer().close();

        try {
            this.getRestClient().close();
        } catch (IOException var2) {
            log.error("unable to close the rest client", var2);
        }

        log.info("shutdown completed");
    }

    public String[] getHosts() {
        return this.hosts;
    }

    public Optional<Integer> getNumThreads() {
        return this.numThreads;
    }

    public RestHighLevelClient getRestClient() {
        return this.restClient;
    }

    public Sniffer getSniffer() {
        return this.sniffer;
    }

    public void setHosts(String[] hosts) {
        this.hosts = hosts;
    }

    public void setNumThreads(Optional<Integer> numThreads) {
        this.numThreads = numThreads;
    }

    public void setRestClient(RestHighLevelClient restClient) {
        this.restClient = restClient;
    }

    public void setSniffer(Sniffer sniffer) {
        this.sniffer = sniffer;
    }

}
