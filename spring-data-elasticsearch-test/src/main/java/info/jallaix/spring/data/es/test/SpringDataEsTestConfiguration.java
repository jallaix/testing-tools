package info.jallaix.spring.data.es.test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.jallaix.spring.data.es.test.util.TestClientOperations;
import info.jallaix.spring.data.es.test.util.TestDocumentsLoader;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.NodeBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.hateoas.hal.Jackson2HalModule;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Spring configuration for Elasticsearch repository tests.
 */
@Configuration
public class SpringDataEsTestConfiguration {

    /**
     * Define the Elasticsearch client, used by the Elasticsearch Test framework
     *
     * @return The Elasticsearch client
     */
    @Bean
    public Client elasticsearchClient() throws IOException {

        // Clean the testing Elasticsearch index (may be inconsistent)
        Path rootPath = Paths.get("target/test-data");
        if (rootPath.toFile().exists())
            Files.walk(rootPath)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);

        // Configure the testing Elasticsearch index
        NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder();
        nodeBuilder.settings().put("path.home", "target");
        nodeBuilder.settings().put("path.data", "target/test-data");
        nodeBuilder.local(true);

        return nodeBuilder.node().client();
    }

    /**
     * Define the Elastic search operations template, used by the Spring Data framework
     *
     * @return The Elastic search operations template
     */
    @Bean
    public ElasticsearchOperations elasticsearchTemplate() throws IOException {
        return new ElasticsearchTemplate(elasticsearchClient());
    }

    /**
     * Define the bean used to load data in an Elasticsearch index before each test
     *
     * @return The test documents loader
     */
    @Bean
    public TestDocumentsLoader testDocumentsLoader() throws IOException {
        return new TestDocumentsLoader(elasticsearchClient());
    }

    /**
     * Define the bean used to perform low level Elasticsearch operations
     *
     * @return The test client operations
     */
    @Bean
    public TestClientOperations testClientOperations() throws IOException {
        return new TestClientOperations(elasticsearchClient());
    }

    /**
     * Define a HAL REST template.
     *
     * @return A HAL REST template
     */
    @Bean
    public RestTemplate halRestTemplate() {

        List<HttpMessageConverter<?>> messageConverters = new ArrayList<HttpMessageConverter<?>>();

        // Configure Jackson mapper for Jackson converter
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new Jackson2HalModule());

        // Configure Jackson converter
        MappingJackson2HttpMessageConverter jacksonConverter = new MappingJackson2HttpMessageConverter();
        jacksonConverter.setSupportedMediaTypes(MediaType.parseMediaTypes("application/json,application/hal+json,application/patch+json"));
        jacksonConverter.setObjectMapper(mapper);
        messageConverters.add(jacksonConverter);

        // Configure String converter
        messageConverters.add(new StringHttpMessageConverter());

        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
        restTemplate.setMessageConverters(messageConverters);

        return restTemplate;
    }
}
