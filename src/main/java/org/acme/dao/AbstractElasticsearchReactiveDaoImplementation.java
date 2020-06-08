package org.acme.dao;

import org.acme.service.AbstractElasticsearchService;
import org.acme.utils.JsonUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.inject.Inject;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class AbstractElasticsearchReactiveDaoImplementation<T> {
    private static final Logger log = LoggerFactory.getLogger(AbstractElasticsearchReactiveDaoImplementation.class);
    public static final char COMMA = ',';
    public static final char DOT = '.';
    public static final String SINGLE_QUOTE = "'";
    public static final String DELETED = "DELETED";
    public static final String TYPE = "type";
    public static final String TEXT = "text";
    public static final String ANALYZER = "analyzer";
    public static final String KEYWORD = "keyword";
    public static final String PROPERTIES = "properties";
    public static final String DATE = "date";
    public static final String FORMAT = "format";
    public static final String LONG = "long";
    public static final String BOOLEAN = "boolean";
    @Inject
    AbstractElasticsearchService abstractElasticsearchService;
    @Inject
    JsonUtils jsonUtils;
    private RestHighLevelClient restClient;

    public AbstractElasticsearchReactiveDaoImplementation() {
    }

    protected abstract String getId(T var1);

    protected abstract void setId(T var1, String var2);

    protected abstract String getTableName();

    protected abstract Class<T> getEntityClass();

    protected abstract Long getVersion(T var1);

    protected abstract void setVersion(T var1, Long var2);

    public void init() {
        log.info("init started");
        this.restClient = this.getAbstractElasticsearchService().getRestClient();
        this.ensureIndex().doOnNext((aBoolean) -> {
            log.info("index {} exists={}", this.getTableName(), aBoolean);
        }).filter((aBoolean) -> {
            return (boolean) aBoolean;
        }).flatMap((aBoolean) -> {
            return this.createIndex();
        }).doOnNext((aBoolean) -> {
            log.info("index {} created={}", this.getTableName(), aBoolean);
        }).doOnTerminate(() -> {
            log.info("init completed");
        }).block();
    }

    public Mono<Object> deleteById(T data) {
        return Mono.just(this.getId(data)).filter((s) -> {
            return StringUtils.isNotBlank(s);
        }).flatMap((s) -> {
            return Flux.create((fluxSink) -> {
                DeleteRequest request = new DeleteRequest(this.getTableName(), s);
                this.getRestClient().deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() {
                    public void onResponse(DeleteResponse deleteResponse) {
                        AbstractElasticsearchReactiveDaoImplementation.log.debug("delete result: {}", deleteResponse);
                        fluxSink.next(StringUtils.equalsAnyIgnoreCase("DELETED", new CharSequence[]{deleteResponse.getResult().getLowercase()}));
                        fluxSink.complete();
                    }

                    public void onFailure(Exception e) {
                        AbstractElasticsearchReactiveDaoImplementation.log.error("unable to delete", e);
                        fluxSink.error(new RuntimeException(e));
                    }
                });
            }).next();
        }).defaultIfEmpty(false);
    }

    public Mono<T> findById(T pkData) {
        return (Mono<T>) Mono.just(this.getId(pkData)).filter((s) -> {
            return StringUtils.isNotBlank(s);
        }).flatMap((s) -> {
            return Flux.create((fluxSink) -> {
                GetRequest request = new GetRequest(this.getTableName(), s);
                this.getRestClient().getAsync(request, RequestOptions.DEFAULT, new ActionListener<GetResponse>() {
                    public void onResponse(GetResponse response) {
                        AbstractElasticsearchReactiveDaoImplementation.log.debug("get result: {}", response);
                        if (response.isSourceEmpty()) {
                            fluxSink.complete();
                        } else {
                            Map map = response.getSourceAsMap();
                            Object result = AbstractElasticsearchReactiveDaoImplementation.this.getJsonUtils().fromMap(map, AbstractElasticsearchReactiveDaoImplementation.this.getEntityClass());
                            fluxSink.next(result);
                            fluxSink.complete();
                        }
                    }

                    public void onFailure(Exception e) {
                        AbstractElasticsearchReactiveDaoImplementation.log.error("unable to get", e);
                        fluxSink.error(new RuntimeException(e));
                    }
                });
            }).next();
        });
    }

    protected Flux<T> findByMatch(Map<String, Object> map) {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        return this.findBy(map, query, (queryBuilder) -> {
            return query.should(queryBuilder);
        }, (s, o) -> {
            return this.getSearchSourceBuilderByMatch(s, o);
        });
    }

    protected Flux<T> findByMatch(String fieldName, Object value) {
        SearchRequest searchRequest = this.getSearchRequestByMatch(fieldName, value);
        return this.search(searchRequest);
    }

    protected SearchRequest getSearchRequestByMatch(String fieldName, Object value) {
        SearchRequest searchRequest = new SearchRequest(new String[]{this.getTableName()});
        SearchSourceBuilder searchSourceBuilder = this.getSearchSourceBuilderByMatch(fieldName, value);
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    protected SearchSourceBuilder getSearchSourceBuilderByMatch(String fieldName, Object value) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(fieldName, value));
        return searchSourceBuilder;
    }

    protected Flux<T> findByExactMatch(Map<String, Object> map) {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        return this.findBy(map, query, (queryBuilder) -> {
            return query.must(queryBuilder);
        }, (s, o) -> {
            return this.getSearchSourceBuilderByExactMatch(s, o);
        });
    }

    protected Flux<T> findBy(Map<String, Object> map, BoolQueryBuilder query, Function<QueryBuilder, BoolQueryBuilder> fn, BiFunction<String, Object, SearchSourceBuilder> fn1) {
        SearchRequest searchRequest = new SearchRequest(new String[]{this.getTableName()});
        SearchSourceBuilder searchSourceBuilder1 = new SearchSourceBuilder();
        searchSourceBuilder1.query(query);
        searchRequest.source(searchSourceBuilder1);
        map.entrySet().stream().filter((entry) -> {
            return StringUtils.isNotBlank((CharSequence)entry.getKey()) && entry.getValue() != null;
        }).map((entry) -> {
            return (SearchSourceBuilder)fn1.apply(entry.getKey(), entry.getValue());
        }).map((searchSourceBuilder) -> {
            return searchSourceBuilder.query();
        }).forEach((queryBuilder) -> {
            BoolQueryBuilder var10000 = (BoolQueryBuilder)fn.apply(queryBuilder);
        });
        return this.search(searchRequest);
    }

    protected Flux<T> findByExactMatch(String fieldName, Object value) {
        SearchRequest searchRequest = this.getSearchRequestByExactMatch(fieldName, value);
        return this.search(searchRequest);
    }

    protected SearchRequest getSearchRequestByExactMatch(String fieldName, Object value) {
        SearchRequest searchRequest = new SearchRequest(new String[]{this.getTableName()});
        SearchSourceBuilder searchSourceBuilder = this.getSearchSourceBuilderByExactMatch(fieldName, value);
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    protected SearchSourceBuilder getSearchSourceBuilderByExactMatch(String fieldName, Object value) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery(fieldName, value));
        return searchSourceBuilder;
    }

    protected Flux<T> search(SearchRequest searchRequest) {
        return Flux.create((fluxSink) -> {
            this.getRestClient().searchAsync(searchRequest, RequestOptions.DEFAULT, new ActionListener<SearchResponse>() {
                public void onResponse(SearchResponse searchResponse) {
                    SearchHits hits = searchResponse.getHits();
                    Arrays.stream(hits.getHits()).forEach((fields) -> {
                        Map<String, Object> map = fields.getSourceAsMap();
                        T t = AbstractElasticsearchReactiveDaoImplementation.this.getJsonUtils().fromMap(map, AbstractElasticsearchReactiveDaoImplementation.this.getEntityClass());
                        fluxSink.next(t);
                    });
                    fluxSink.complete();
                }

                public void onFailure(Exception e) {
                    AbstractElasticsearchReactiveDaoImplementation.log.error("search failed", e);
                    fluxSink.error(new RuntimeException(e));
                }
            });
        });
    }

    public Mono<T> save(T data) {
        return (Mono<T>) Mono.just(data).flatMap((t) -> {
            if (StringUtils.isBlank(this.getId(t))) {
                this.setId(t, UUID.randomUUID().toString());
            }

            IndexRequest request = new IndexRequest(this.getTableName());
            request.id(this.getId(t));
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            Long versionNum = (Long) Optional.ofNullable(this.getVersion(t)).map((aLong) -> {
                return aLong + 1L;
            }).orElse(0L);
            request.version(versionNum);
            request.versionType(VersionType.EXTERNAL);
            this.setVersion(t, versionNum);
            return Mono.just(this.getJsonUtils().toStringLazy(t).toString()).flatMapMany((s) -> {
                return Flux.create((fluxSink) -> {
                    request.source(s, XContentType.JSON);
                    this.getRestClient().indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
                        public void onResponse(IndexResponse response) {
                            AbstractElasticsearchReactiveDaoImplementation.log.debug("save result: {}", response);
                            fluxSink.next(t);
                            fluxSink.complete();
                        }

                        public void onFailure(Exception e) {
                            AbstractElasticsearchReactiveDaoImplementation.log.error("unable to save", e);
                            fluxSink.error(new RuntimeException(e));
                        }
                    });
                });
            }).next();
        });
    }

    public Mono<T> update(T data) {
        if (data == null) {
            return Mono.empty();
        } else {
            String id = this.getId(data);
            if (StringUtils.isBlank(id)) {
                return Mono.error(new RuntimeException("id is blank"));
            } else {
                UpdateRequest request = new UpdateRequest(this.getTableName(), id);
                request.doc(this.getJsonUtils().toStringLazy(data).toString(), XContentType.JSON);
                request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
                request.retryOnConflict(5);
                request.fetchSource(true);
                return Flux.create((fluxSink) -> {
                    this.getRestClient().updateAsync(request, RequestOptions.DEFAULT, new ActionListener<UpdateResponse>() {
                        public void onResponse(UpdateResponse response) {
                            GetResult result = response.getGetResult();
                            if (result.isExists() && !result.isSourceEmpty()) {
                                String sourceAsString = result.sourceAsString();
                                AbstractElasticsearchReactiveDaoImplementation.this.getJsonUtils().readValue(sourceAsString, AbstractElasticsearchReactiveDaoImplementation.this.getEntityClass()).ifPresent((t) -> {
                                    AbstractElasticsearchReactiveDaoImplementation.this.setVersion(t, response.getVersion());
                                    fluxSink.next(t);
                                });
                            }

                            fluxSink.complete();
                        }

                        public void onFailure(Exception e) {
                            AbstractElasticsearchReactiveDaoImplementation.log.error("unable to update", e);
                            fluxSink.error(new RuntimeException(e));
                        }
                    });
                }).next().flatMap((t) -> {
                    return this.save((T) t);
                });
            }
        }
    }

    protected Map<String, Object> getKeywordTextAnalizer() {
        return this.getTextAnalizer("keyword");
    }

    protected Map<String, Object> getTextAnalizer(String analyzer) {
        Map<String, Object> result = new HashMap();
        result.put("type", "text");
        result.put("analyzer", analyzer);
        return result;
    }

    protected Map<String, Object> getDateFieldType() {
        Map<String, Object> result = new HashMap();
        result.put("type", "date");
        result.put("format", "strict_date_time||epoch_second||epoch_millis");
        return result;
    }

    protected Map<String, Object> getLongFieldType() {
        Map<String, Object> result = new HashMap();
        result.put("type", "long");
        return result;
    }

    protected Map<String, Object> getBooleanFieldType() {
        Map<String, Object> result = new HashMap();
        result.put("type", "boolean");
        return result;
    }

    protected Mono<Object> createIndex() {
        CreateIndexRequest request = new CreateIndexRequest(this.getTableName());
        return this.createIndex(request);
    }

    protected Mono<Object> createIndex(CreateIndexRequest request) {
        return Flux.create((fluxSink) -> {
            this.getRestClient().indices().createAsync(request, RequestOptions.DEFAULT, new ActionListener<CreateIndexResponse>() {
                public void onResponse(CreateIndexResponse createIndexResponse) {
                    AbstractElasticsearchReactiveDaoImplementation.log.info("CreateIndexResponse: {}", createIndexResponse);
                    fluxSink.next(createIndexResponse.isAcknowledged());
                    fluxSink.complete();
                }

                public void onFailure(Exception e) {
                    AbstractElasticsearchReactiveDaoImplementation.log.error("unable to create index", e);
                    fluxSink.error(new RuntimeException(e));
                }
            });
        }).next();
    }

    protected Mono<Object> ensureIndex() {
        return Mono.just(false).flatMap((atomicBoolean) -> {
            return Flux.create((fluxSink) -> {
                this.getRestClient().getLowLevelClient().performRequestAsync(new Request("HEAD", this.getTableName()), new ResponseListener() {
                    public void onSuccess(Response response) {
                        AbstractElasticsearchReactiveDaoImplementation.this.logResponse(response);
                        boolean result = response.getStatusLine().getStatusCode() == 200;
                        fluxSink.next(result);
                        fluxSink.complete();
                    }

                    public void onFailure(Exception exception) {
                        AbstractElasticsearchReactiveDaoImplementation.log.error("unable to check for index", exception);
                        fluxSink.error(new RuntimeException(exception));
                    }
                });
            }).next();
        });
    }

    private void logResponse(Response response) {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            log.debug("entity is null");
        } else {
            try {
                InputStream content = entity.getContent();
                log.debug("response result: {}", IOUtils.toString(content, StandardCharsets.UTF_8));
            } catch (Exception var4) {
                log.error("unable to log response", var4);
            }

        }
    }

    public AbstractElasticsearchService getAbstractElasticsearchService() {
        return this.abstractElasticsearchService;
    }

    public JsonUtils getJsonUtils() {
        return this.jsonUtils;
    }

    public RestHighLevelClient getRestClient() {
        return this.restClient;
    }

    public void setAbstractElasticsearchService(AbstractElasticsearchService abstractElasticsearchService) {
        this.abstractElasticsearchService = abstractElasticsearchService;
    }

    public void setJsonUtils(JsonUtils jsonUtils) {
        this.jsonUtils = jsonUtils;
    }

    public void setRestClient(RestHighLevelClient restClient) {
        this.restClient = restClient;
    }
}
