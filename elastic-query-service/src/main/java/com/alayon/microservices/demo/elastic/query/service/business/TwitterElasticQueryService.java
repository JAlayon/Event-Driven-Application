package com.alayon.microservices.demo.elastic.query.service.business;

import com.alayon.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import com.alayon.microservices.demo.elastic.query.client.service.ElasticQueryClient;
import com.alayon.microservices.demo.elastic.query.service.model.ElasticQueryServiceResponseModel;
import com.alayon.microservices.demo.elastic.query.service.model.assembler.ElasticQueryServiceResponseModelAssembler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
class TwitterElasticQueryService implements ElasticQueryService{

    private static final Logger log = LoggerFactory.getLogger(TwitterElasticQueryService.class);

    private final ElasticQueryServiceResponseModelAssembler assembler;

    private final ElasticQueryClient<TwitterIndexModel> elasticQueryClient;

    public TwitterElasticQueryService(final ElasticQueryServiceResponseModelAssembler assembler,
                                      final ElasticQueryClient<TwitterIndexModel> elasticQueryClient) {
        this.assembler = assembler;
        this.elasticQueryClient = elasticQueryClient;
    }

    @Override
    public ElasticQueryServiceResponseModel getDocumentById(final String id) {
        log.info("Querying elasticsearch by id: {}", id);
        return assembler.toModel(elasticQueryClient.getIndexModelById(id));
    }

    @Override
    public List<ElasticQueryServiceResponseModel> getDocumentByText(final String text) {
        log.info("Querying elasticsearch by text: {}", text);
        return assembler.toModels(elasticQueryClient.getIndexModelByText(text));
    }

    @Override
    public List<ElasticQueryServiceResponseModel> getAllDocuments() {
        log.info("Querying all documents in elasticsearch");
        return assembler.toModels(elasticQueryClient.getAllIndexModels());
    }
}
