package info.jallaix.spring.data.es.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.annotations.Document;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * This class is used to performed low level operations on an Elasticsearch index.
 * Unit tests may perform index operations without using Spring Data.
 */
public class TestClientOperations {

    /**
     * Elasticsearch client
     */
    private Client esClient;

    /**
     * Logger
     */
    private static final Logger logger = LoggerFactory.getLogger(TestClientOperations.class);


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                               Public methods                                                   */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Constructor with Elasticsearch client
     * @param esClient the Elasticsearch client
     */
    public TestClientOperations(Client esClient) { this.esClient = esClient; }

    /**
     * Count the number of typed documents in the index.
     * @param documentMetadata The Elastic document metadata
     * @return The number of typed documents found
     */
    public long countDocuments(Document documentMetadata) {

        return esClient.prepareCount(documentMetadata.indexName())
                .setTypes(documentMetadata.type())
                .get()
                .getCount();
    }

    /**
     * Find all typed documents in the index.
     * @param <T> A typed document type
     * @param documentMetadata The Elastic document metadata
     * @param documentClass The document class
     * @return The typed documents found
     */
    public <T> List<T> findAllDocuments(Document documentMetadata, Class<T> documentClass) {

        List<T> documents = new ArrayList<>();

        esClient.prepareSearch(documentMetadata.indexName())
                .setTypes(documentMetadata.type())
                .execute()
                .actionGet()
                .getHits()
                .forEach(hit -> documents.add(fromJson(hit, documentClass)));

        return documents;
    }

    /**
     * Find all typed document belonging to a page with sorting
     * @param <T> A typed document type
     * @param documentMetadata The Elastic document metadata
     * @param documentClass The document class
     * @param documentIdField The document identifier field
     * @param documentSortField The document sort field
     * @param pageNo The page number to get
     * @param pageSize The page size
     * @return The typed documents found
     */
    public <T> List<T> findAllDocumentsByPage(Document documentMetadata, Class<T> documentClass, Field documentIdField, Field documentSortField, int pageNo, int pageSize) {

        List<T> documents = new ArrayList<>();

        esClient.prepareSearch(documentMetadata.indexName())
                .setTypes(documentMetadata.type())
                .setFrom(pageNo * pageSize)
                .setSize(pageSize)
                .addSort(documentSortField.getName(), SortOrder.DESC)
                .execute()
                .actionGet()
                .getHits()
                .forEach(hit -> documents.add(fromJson(hit, documentClass)));

        return documents;
    }

    /**
     * Find all typed document with sorting
     * @param <T> A typed document type
     * @param documentMetadata The Elastic document metadata
     * @param documentClass The document class
     * @param documentIdField The document identifier
     * @param documentSortField The document sort field
     * @return The typed documents found
     */
    public <T> List<T> findAllDocumentsSorted(Document documentMetadata, Class<T> documentClass, Field documentIdField, Field documentSortField) {

        List<T> documents = new ArrayList<>();

        esClient.prepareSearch(documentMetadata.indexName())
                .setTypes(documentMetadata.type())
                .addSort(documentSortField.getName(), SortOrder.DESC)
                .execute()
                .actionGet()
                .getHits()
                .forEach(hit -> documents.add(fromJson(hit, documentClass)));

        return documents;
    }

    /**
     * Convert an Elasticsearch hit to an entity
     * @param <T> A typed document type
     * @param hit The search hit
     * @param documentClass The document class
     * @return The entity
     */
    private <T> T fromJson(SearchHit hit, Class<T> documentClass) {

        try {
            return new ObjectMapper().readValue(hit.getSourceAsString(), documentClass);
        } catch (IOException e) {
            logger.error(null, e);
            return null;
        }
    }
}
