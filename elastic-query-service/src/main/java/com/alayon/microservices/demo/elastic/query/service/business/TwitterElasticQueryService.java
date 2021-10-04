package com.alayon.microservices.demo.elastic.query.service.business;

import com.alayon.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import com.alayon.microservices.demo.elastic.query.client.service.ElasticQueryClient;
import com.alayon.microservices.demo.elastic.query.service.model.ElasticQueryServiceResponseModel;
import com.alayon.microservices.demo.elastic.query.service.transformer.ElasticToResponseModelTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
class TwitterElasticQueryService implements ElasticQueryService{

    private static final Logger log = LoggerFactory.getLogger(TwitterElasticQueryService.class);

    private final ElasticToResponseModelTransformer transformer;

    private final ElasticQueryClient<TwitterIndexModel> elasticQueryClient;

    public TwitterElasticQueryService(final ElasticToResponseModelTransformer transformer,
                                      final ElasticQueryClient<TwitterIndexModel> elasticQueryClient) {
        this.transformer = transformer;
        this.elasticQueryClient = elasticQueryClient;
    }

    @Override
    public ElasticQueryServiceResponseModel getDocumentById(final String id) {
        log.info("Querying elasticsearch by id: {}", id);
        return transformer.getResponseModel(elasticQueryClient.getIndexModelById(id));
    }

    @Override
    public List<ElasticQueryServiceResponseModel> getDocumentByText(final String text) {
        log.info("Querying elasticsearch by text: {}", text);
        return transformer.getResponseModels(elasticQueryClient.getIndexModelByText(text));
    }

    @Override
    public List<ElasticQueryServiceResponseModel> getAllDocuments() {
        log.info("Querying all documents in elasticsearch");
        return transformer.getResponseModels(elasticQueryClient.getAllIndexModels());
    }
}
