package com.alayon.microservices.demo.elastic.query.client.service.impl;

import com.alayon.microservices.demo.common.util.CollectionsUtil;
import com.alayon.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import com.alayon.microservices.demo.elastic.query.client.exception.ElasticQueryClientException;
import com.alayon.microservices.demo.elastic.query.client.repository.TwitterElasticsearchQueryRepository;
import com.alayon.microservices.demo.elastic.query.client.service.ElasticQueryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.util.List;

@Primary
@Service
public class TwitterElasticRepositoryQueryClient implements ElasticQueryClient<TwitterIndexModel> {

    private static final Logger log = LoggerFactory.getLogger(TwitterElasticRepositoryQueryClient.class);

    private final TwitterElasticsearchQueryRepository repository;

    public TwitterElasticRepositoryQueryClient(final TwitterElasticsearchQueryRepository repository) {
        this.repository = repository;
    }

    @Override
    public TwitterIndexModel getIndexModelById(final String id) {
        var searchResult = repository.findById(id);

        searchResult.ifPresentOrElse(model -> {
            log.info("Document with id {} retrieved successfully",model.getId());
        }, () -> {
            throw new ElasticQueryClientException("No document found at elasticsearch with id: " + id);
        });
        return searchResult.get();
    }

    @Override
    public List<TwitterIndexModel> getIndexModelByText(final String text) {
        var searchResult = repository.findByText(text);
        log.info("{} of documents with text {} retrieved successfully", searchResult.size(),text);
        return searchResult;
    }

    @Override
    public List<TwitterIndexModel> getAllIndexModels() {
        var iterable = repository.findAll();
       /* var searchResult = StreamSupport.stream(iterable.spliterator(), false)
                .collect(Collectors.toList());*/
        var searchResult = CollectionsUtil.getInstance().getListFromIterable(iterable);
        log.info("{} number of documents retrieved successfully", searchResult.size());
        return searchResult;
    }
}
