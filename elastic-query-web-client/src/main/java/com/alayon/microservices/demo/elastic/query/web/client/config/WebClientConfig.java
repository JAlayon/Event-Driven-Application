package com.alayon.microservices.demo.elastic.query.web.client.config;

import com.alayon.microservices.demo.config.ElasticQueryWebClientConfigData;
import com.alayon.microservices.demo.config.UserConfigData;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.ExchangeFilterFunctions;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {

    private final ElasticQueryWebClientConfigData.WebClient webClientConfigData;

    private final UserConfigData userConfigData;

    public WebClientConfig(final ElasticQueryWebClientConfigData webClientConfigData,
                           final UserConfigData userConfigData) {
        this.webClientConfigData = webClientConfigData.getWebClient();
        this.userConfigData = userConfigData;
    }

    @LoadBalanced
    @Bean("webClientBuilder")
    WebClient.Builder webClientBuilder(){
        return WebClient.builder()
                .filter(ExchangeFilterFunctions
                        .basicAuthentication(userConfigData.getUsername(), userConfigData.getPassword()))
                .baseUrl(webClientConfigData.getBaseUrl())
                .defaultHeader(HttpHeaders.CONTENT_TYPE, webClientConfigData.getContentType())
                .defaultHeader(HttpHeaders.ACCEPT, webClientConfigData.getAcceptType())
                .codecs(clientCodecConfigurer ->  {
                    clientCodecConfigurer
                            .defaultCodecs()
                            .maxInMemorySize(webClientConfigData.getMaxInMemorySize());
                });
    }

}
