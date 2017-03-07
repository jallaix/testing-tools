package info.jallaix.spring.data.es.test.util;

import info.jallaix.spring.data.es.test.bean.DocumentMetaData;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.SearchQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * This class is used to load documents in an Elasticsearch index.
 * Unit tests may perform index operations against these data.
 */
public class TestDocumentsLoader {

    /**
     * Elasticsearch operations
     */
    private ElasticsearchOperations esOperations;

    /**
     * Number of documents loaded in the index
     */
    private long loadedDocumentCount = 0;

    /**
     * Logger
     */
    private static final Logger logger = LoggerFactory.getLogger(TestDocumentsLoader.class);


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                               Public methods                                                   */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Constructor with Elasticsearch operations.
     *
     * @param esOperations The Elasticsearch operations
     */
    public TestDocumentsLoader(ElasticsearchOperations esOperations) {
        this.esOperations = esOperations;
    }

    /**
     * Create an Elasticsearch index with sample documents
     *
     * @param documentMetaData Elastic document metadata
     * @param entityClasses    Set of entity classes used for mapping
     * @param documentsToStore Tested class name
     */
    public void initElasticIndex(DocumentMetaData<?> documentMetaData, Set<Class<?>> entityClasses, List<?> documentsToStore) {

        for (Class<?> entityClass : entityClasses) {

            // Clean the index data
            if (esOperations.indexExists(entityClass))
                esOperations.deleteIndex(entityClass);

            // Apply mapping for the document type
            esOperations.putMapping(entityClass);
        }

        List<IndexQuery> indexQueries = new ArrayList<IndexQuery>();
        for (Object documentToStore : documentsToStore) {
            DocumentMetaData<?> documentToStoreMetaData = DocumentMetaDataBuilder.buildDocumentMetadata(documentToStore.getClass());
            IndexQueryBuilder indexQueryBuilder = new IndexQueryBuilder();
            try {
                indexQueryBuilder.withId(String.class.cast(documentToStoreMetaData.getDocumentIdField().get(documentToStore)));
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            indexQueryBuilder.withObject(documentsToStore);

            indexQueries.add(indexQueryBuilder.build());
        }
        esOperations.bulkIndex(indexQueries);

        // Number of loaded documents
        loadedDocumentCount = countLoadedDocuments(
                documentMetaData.getDocumentAnnotation().indexName(),
                documentMetaData.getDocumentAnnotation().type());
    }

    /**
     * Free resources used by Elasticsearch
     */
    public void terminateElasticIndex() {
        /*if (esSetup != null)
            esSetup.terminate();*/
    }

    /**
     * Get the number of loaded documents
     *
     * @return The number of loaded documents
     */
    public long getLoadedDocumentCount() {
        return loadedDocumentCount;
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                              Private methods                                                   */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Count the number of documents loaded in the index
     *
     * @param indexName    Index name
     * @param documentType Document type
     */
    private long countLoadedDocuments(String indexName, String documentType) {
        SearchQuery query = new NativeSearchQuery(QueryBuilders.matchAllQuery());
        query.addIndices(indexName);
        query.addTypes(documentType);
        return esOperations.count(query);
    }
}
