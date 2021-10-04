package com.alayon.microservices.demo.elastic.query.service.api;

import com.alayon.microservices.demo.elastic.query.service.business.ElasticQueryService;
import com.alayon.microservices.demo.elastic.query.service.model.ElasticQueryServiceRequestModel;
import com.alayon.microservices.demo.elastic.query.service.model.ElasticQueryServiceResponseModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.util.List;

@RestController
@RequestMapping(value = "/documents")
public class ElasticDocumentController {

    private static final Logger log = LoggerFactory.getLogger(ElasticDocumentController.class);

    private final ElasticQueryService elasticQueryService;

    public ElasticDocumentController(final ElasticQueryService elasticQueryService) {
        this.elasticQueryService = elasticQueryService;
    }

    @GetMapping("/")
    public ResponseEntity<List<ElasticQueryServiceResponseModel>>
    getAllDocuments(){
        var response = elasticQueryService.getAllDocuments();
        log.info("Elasticsearch returned {} of documents", response.size());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/{id}")
    public ResponseEntity<ElasticQueryServiceResponseModel>
    getDocumentById(@PathVariable @NotEmpty String id){
        var response = elasticQueryService.getDocumentById(id);
        log.info("Elasticsearch returned document with id {}", id);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/get-document-by-text")
    public ResponseEntity<List<ElasticQueryServiceResponseModel>>
    getDocumentByText(
            @Valid
            @RequestBody ElasticQueryServiceRequestModel request){
        var response = elasticQueryService.getDocumentByText(request.getText());
        log.info("Elasticsearch returned {} of documents", response.size());
        return ResponseEntity.ok(response);
    }
}
