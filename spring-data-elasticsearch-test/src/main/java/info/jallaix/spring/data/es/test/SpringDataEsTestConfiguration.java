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
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;

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
    public Client elasticsearchClient() {

        NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder();
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
    public ElasticsearchOperations elasticsearchTemplate() {
        return new ElasticsearchTemplate(elasticsearchClient());
    }

    /**
     * Define the bean used to load data in an Elasticsearch index before each test
     *
     * @return The test documents loader
     */
    @Bean
    public TestDocumentsLoader testDocumentsLoader() {
        return new TestDocumentsLoader(elasticsearchClient());
    }

    /**
     * Define the bean used to perform low level Elasticsearch operations
     *
     * @return The test client operations
     */
    @Bean
    public TestClientOperations testClientOperations() {
        return new TestClientOperations(elasticsearchClient());
    }

    /**
     * Define a HAL REST template.
     *
     * @return A HAL REST template
     */
    @Bean
    public RestTemplate halRestTemplate() {

        // Configure Jackson mapper
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new Jackson2HalModule());

        // Configure Jackson converter
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setSupportedMediaTypes(MediaType.parseMediaTypes("application/hal+json"));
        converter.setObjectMapper(mapper);

        return new RestTemplate(Collections.<HttpMessageConverter<?>>singletonList(converter));
    }
}
