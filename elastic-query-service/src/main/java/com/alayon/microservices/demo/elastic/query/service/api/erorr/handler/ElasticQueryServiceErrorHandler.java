package com.alayon.microservices.demo.elastic.query.service.api.erorr.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.HashMap;
import java.util.Map;

@ControllerAdvice
public class ElasticQueryServiceErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(ElasticQueryServiceErrorHandler.class);

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<String> handleAccessDeniedException(AccessDeniedException e){
        log.error("Access denied exception!", e);
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body("You are not authorized to access this resource");
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, String>> handleValidationErrors(MethodArgumentNotValidException e){
        log.error("Method argument validation exception!", e);
        var map = new HashMap<String,String>();
        e.getBindingResult().getAllErrors().forEach(
                error -> map.put(((FieldError) error).getField(), error.getDefaultMessage())
        );
        return ResponseEntity.badRequest().body(map);
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ResponseEntity<String> handleGlobalExceptions(Exception e){
        log.error("Internal server error!", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("A server error occurred!");
    }


}
