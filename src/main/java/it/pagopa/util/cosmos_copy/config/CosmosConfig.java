package it.pagopa.util.cosmos_copy.config;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CosmosConfig {

    @Value("${azure.cosmos.uri}")
    private String cosmosEndpoint;

    @Value("${azure.cosmos.key}")
    private String cosmosKey;

    @Bean
    public CosmosAsyncClient cosmosAsyncClient() {
        return new CosmosClientBuilder()
                .endpoint(cosmosEndpoint)
                .key(cosmosKey)
                .contentResponseOnWriteEnabled(true)
                .gatewayMode()
                .buildAsyncClient();
    }
}