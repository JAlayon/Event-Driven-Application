package com.alayon.microservices.demo.elastic.index.client.service.impl;

import com.alayon.microservices.demo.config.ElasticConfigData;
import com.alayon.microservices.demo.elastic.index.client.service.ElasticIndexClient;
import com.alayon.microservices.demo.elastic.index.client.util.ElasticIndexUtil;
import com.alayon.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexedObjectInformation;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@ConditionalOnProperty(name = "elastic-config.is-repository", havingValue = "false")
public class TwitterElasticIndexClient implements ElasticIndexClient<TwitterIndexModel> {

    private static final Logger log = LoggerFactory.getLogger(TwitterElasticIndexClient.class);

    private final ElasticConfigData elasticConfigData;

    private final ElasticsearchOperations elasticsearchOperations;

    private final ElasticIndexUtil<TwitterIndexModel> elasticIndexUtil;

    public TwitterElasticIndexClient(final ElasticConfigData elasticConfigData,
                                     final ElasticsearchOperations elasticsearchOperations,
                                     final ElasticIndexUtil<TwitterIndexModel> elasticIndexUtil) {
        this.elasticConfigData = elasticConfigData;
        this.elasticsearchOperations = elasticsearchOperations;
        this.elasticIndexUtil = elasticIndexUtil;
    }

    @Override
    public List<String> save(final List<TwitterIndexModel> documents) {
        List<IndexQuery> indexQueries = elasticIndexUtil.getIndexQueries(documents);
        List<IndexedObjectInformation> indexedObjectsInformation = elasticsearchOperations.bulkIndex(
                indexQueries,
                IndexCoordinates.of(elasticConfigData.getIndexName())
        );
        List<String> documentIds = indexedObjectsInformation.
                                    stream()
                                    .map(IndexedObjectInformation::getId)
                                    .collect(Collectors.toList());
        log.info("Documents indexed successfully with type: {} and Ids: {}",
                TwitterIndexModel.class.getName(), documentIds);
        return documentIds;
    }
}
