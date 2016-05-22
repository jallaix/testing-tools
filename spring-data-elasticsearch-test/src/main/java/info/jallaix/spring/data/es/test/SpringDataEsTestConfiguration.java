package info.jallaix.spring.data.es.test;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.NodeBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

/**
 * Spring configuration for Elasticsearch repository tests
 */
@Configuration
public class SpringDataEsTestConfiguration {

    /**
     * Define the Elasticsearch client, used by the Elasticsearch Test framework
     * @return The Elasticsearch client
     */
    @Bean
    public Client elasticsearchClient() {

        NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder();
        nodeBuilder.local(true);

        return nodeBuilder.node().client();
    }

    /**
     * Define the Elastic search operations template, used by the Spring Data framework
     * @return The Elastic search operations template
     */
    @Bean
    public ElasticsearchOperations elasticsearchTemplate() {

        return new ElasticsearchTemplate(elasticsearchClient());
    }

    /**
     * Define the bean used to load data in an Elasticsearch index before each test
     * @return The test documents loader
     */
    @Bean
    public TestDocumentsLoader testDocumentsLoader() {

        return new TestDocumentsLoader(elasticsearchClient());
    }

    /**
     * Define the bean used to perform low level Elasticsearch operations
     * @return The test client operations
     */
    @Bean
    public TestClientOperations testClientOperations() {

        return new TestClientOperations(elasticsearchClient());
    }
}
