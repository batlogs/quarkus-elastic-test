package org.acme.service;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.acme.dao.DocumentDaoImpl;
import org.acme.domain.Document;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
@Getter
@Setter
@Slf4j
public class ExampleService {

    @Inject
    DocumentDaoImpl dao;

    public String greet(String name) {
        return "Hello " + name+"!";
    }


    public Mono<Document> save(Document entity) {
        return getDao().save(entity);
    }

    public Mono<Document> findById(Document entity) {
        return getDao().findById(entity);
    }

    public Mono<Document> findById(String id) {
        return Mono.just(Document.of(id, null, null, null))
                .flatMap(entity -> findById(entity))
                ;
    }


    public Flux<Document> findByName(String value) {
        return getDao().findByName(value);
    }

    public Flux<Document> findByDescription(String value) {
        return getDao().findByDescription(value);
    }

    public Flux<Document> findByNameOrDescription(String value) {
        return getDao().findByNameOrDescription(value);
    }

    public Mono<Object> delete(Document entity) {
        return Mono.just(entity.getId())
                .filter(s -> StringUtils.isNotBlank(s))
                .flatMap(s -> getDao().deleteById(entity))
                .defaultIfEmpty(false);
    }

}
