package info.jallaix.spring.data.es.test.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import info.jallaix.spring.data.es.test.bean.DocumentMetaData;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * <p/>
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
     *
     * @param esClient the Elasticsearch client
     */
    public TestClientOperations(Client esClient) {
        this.esClient = esClient;
    }

    /**
     * Count the number of typed documents in the index.
     *
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
     * Find a single a document in the index.
     *
     * @param documentClass The document class
     * @param id The document id
     * @param <T> The document type
     * @return The found document
     */
    public <T> T findDocument(Class<T> documentClass, String id) {

        DocumentMetaData<T> documentMetaData = DocumentMetaDataBuilder.buildDocumentMetadata(documentClass);

        return fromJson(
                documentClass,
                esClient
                        .prepareGet(documentMetaData.getDocumentAnnotation().indexName(), documentMetaData.getDocumentAnnotation().type(), id)
                        .get());
    }

    /**
     * Find all typed documents in the index.
     *
     * @param <T>              The document type
     * @param documentMetaData The Elasticsearch document metadata
     * @return The typed documents found
     */
    public <T> List<T> findAllDocuments(DocumentMetaData<T> documentMetaData) {
        return findAllDocumentsPaged(documentMetaData, 0, 10);
    }

    /**
     * Find all typed document with sorting
     *
     * @param <T>               The document type
     * @param documentMetaData  The Elasticsearch document metadata
     * @param documentSortField The document sort field
     * @return The typed documents found
     */
    public <T> List<T> findAllDocumentsSorted(DocumentMetaData<T> documentMetaData, Field documentSortField) {
        return findAllDocumentsPagedSorted(documentMetaData, documentSortField, 0, 10);
    }

    /**
     * Find all typed document with sorting
     *
     * @param <T>              The document type
     * @param documentMetaData The Elasticsearch document metadata
     * @param pageNo           The page number to get
     * @param pageSize         The page size
     * @return The typed documents found
     */
    public <T> List<T> findAllDocumentsPaged(DocumentMetaData<T> documentMetaData, int pageNo, int pageSize) {

        return StreamSupport.stream(
                esClient.prepareSearch(documentMetaData.getDocumentAnnotation().indexName())
                        .setTypes(documentMetaData.getDocumentAnnotation().type())
                        .setFrom(pageNo * pageSize)
                        .setSize(pageSize)
                        .execute()
                        .actionGet()
                        .getHits()
                        .spliterator(), false)
                .map(hit -> fromJson(documentMetaData.getDocumentClass(), hit))
                .collect(Collectors.toList());
    }

    /**
     * Find all typed document belonging to a page with sorting
     *
     * @param <T>               The document type
     * @param documentMetaData  The Elasticsearch document metadata
     * @param documentSortField The document sort field
     * @param pageNo            The page number to get
     * @param pageSize          The page size
     * @return The typed documents found
     */
    public <T> List<T> findAllDocumentsPagedSorted(DocumentMetaData<T> documentMetaData, Field documentSortField, int pageNo, int pageSize) {

        return StreamSupport.stream(
                esClient.prepareSearch(documentMetaData.getDocumentAnnotation().indexName())
                        .setTypes(documentMetaData.getDocumentAnnotation().type())
                        .setFrom(pageNo * pageSize)
                        .setSize(pageSize)
                        .addSort(documentSortField.getName(), SortOrder.DESC)
                        .execute()
                        .actionGet()
                        .getHits()
                        .spliterator(), false)
                .map(hit -> fromJson(documentMetaData.getDocumentClass(), hit))
                .collect(Collectors.toList());
    }

    /**
     * Convert an Elasticsearch search hit to an entity
     *
     * @param <T>           The document type
     * @param documentClass The document class
     * @param hit           The search hit
     * @return The built entity
     */
    private <T> T fromJson(Class<T> documentClass, SearchHit hit) {
        return fromJson(documentClass, hit.getSourceAsString(), hit.getId());
    }

    /**
     * Convert an Elasticsearch get response to an entity
     *
     * @param <T>           The document type
     * @param documentClass The document class
     * @param response      The get response
     * @return The built entity
     */
    private <T> T fromJson(Class<T> documentClass, GetResponse response) {
        return fromJson(documentClass, response.getSourceAsString(), response.getId());
    }

    private <T> T fromJson(Class<T> documentClass, String jsonSource, String id) {

        T entity;
        try {
            entity = new ObjectMapper().readValue(jsonSource, documentClass);
        } catch (IOException e) {
            logger.error(null, e);
            return null;
        }

        Field idField = Arrays.asList(documentClass.getDeclaredFields()).stream()
                .filter(f -> f.isAnnotationPresent(Id.class))
                .findFirst().orElseThrow(() -> new RuntimeException("Missing @Id annotation in document class " + documentClass.getName()));
        try {
            idField.setAccessible(true);
            idField.set(entity, id);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Impossible to affect value '" + id + "' to " + documentClass.getName() + "." + idField.getName());
        }

        return entity;
    }
}
