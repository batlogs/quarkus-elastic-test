package org.acme.dao;

import io.quarkus.runtime.StartupEvent;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.acme.domain.Document;
import org.acme.service.ElasticService;
import org.acme.utils.JsonUtils;
import org.elasticsearch.client.indices.CreateIndexRequest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
@Slf4j
@Setter
@Getter
public class DocumentDaoImpl extends AbstractElasticsearchReactiveDaoImplementation<Document>{
    public static final String TABLE_NAME_PREFFIX = "document";
    public static final String ENGLISH = "english";

    @Inject
    JsonUtils jsonUtils;
    @Inject
    ElasticService elasticService;

    private String tableName;

    public void init(@Observes StartupEvent event) {
        log.info("init started");
        setTableName(TABLE_NAME_PREFFIX);
        super.init();
    }

    @Override
    protected String getId(Document document) {
        return document.getId();
    }

    @Override
    protected void setId(Document document, String id) {
         document.setId(id);
    }

    @Override
    protected String getTableName() {
        return null;
    }

    @Override
    protected Class<Document> getEntityClass() {
        return Document.class;
    }

    @Override
    protected Long getVersion(Document document) {
        return document.getVersion();
    }

    @Override
    protected void setVersion(Document document, Long version) {
        document.setVersion(version);
    }

    public Flux<Document> findByName(String value) {
        return findByExactMatch(Document.NAME, value);
    }

    public Flux<Document> findByDescription(String value) {
        return findByMatch(Document.DESCRIPTION, value);
    }

    public Flux<Document> findByNameOrDescription(String value) {
        Map<String, Object> query = new HashMap<>();
        query.put(Document.NAME, value);
        query.put(Document.DESCRIPTION, value);
        return findByMatch(query);
    }

    @Override
    protected Mono<Object> createIndex() {
        CreateIndexRequest request = new CreateIndexRequest(getTableName());
        Map<String, Object> mapping = new HashMap();
        Map<String, Object> propsMapping = new HashMap<>();
        propsMapping.put(Document.ID, getKeywordTextAnalizer());
        propsMapping.put(Document.NAME, getTextAnalizer(ENGLISH));
        propsMapping.put(Document.DESCRIPTION, getTextAnalizer(ENGLISH));
        propsMapping.put(Document.VERSION, getLongFieldType());
        mapping.put(PROPERTIES, propsMapping);
        request.mapping(mapping);

        return createIndex(request);
    }

}
