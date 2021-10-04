package com.alayon.microservices.demo.elastic.query.service.api;

import com.alayon.microservices.demo.elastic.query.service.model.ElasticQueryServiceRequestModel;
import com.alayon.microservices.demo.elastic.query.service.model.ElasticQueryServiceResponseModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping(value = "/documents")
public class ElasticDocumentController {

    private static final Logger log = LoggerFactory.getLogger(ElasticDocumentController.class);

    @GetMapping("/")
    public ResponseEntity<List<ElasticQueryServiceResponseModel>>
    getAllDocuments(){
        var response = new ArrayList<ElasticQueryServiceResponseModel>();
        log.info("Elasticsearch returned {} of documents", response.size());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/{id}")
    public ResponseEntity<ElasticQueryServiceResponseModel>
    getDocumentById(@PathVariable String id){
        var response = ElasticQueryServiceResponseModel.builder()
                                            .id(id)
                                            .build();
        log.info("Elasticsearch returned document with id {}", id);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/get-document-by-text")
    public ResponseEntity<List<ElasticQueryServiceResponseModel>>
    getDocumentByText(@RequestBody ElasticQueryServiceRequestModel request){
        var response = new ArrayList<ElasticQueryServiceResponseModel>();
        log.info("Elasticsearch returned {} of documents", response.size());
        return ResponseEntity.ok(response);
    }
}
