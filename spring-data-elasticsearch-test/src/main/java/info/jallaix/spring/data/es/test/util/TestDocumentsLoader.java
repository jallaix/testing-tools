package info.jallaix.spring.data.es.test.util;

import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentEntity;
import org.springframework.data.elasticsearch.core.query.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
     * Create an Elasticsearch index with sample documents.
     *
     * @param documentMetadata Elasticsearch document metadata
     * @param documentsToStore Tested class name
     */
    public void initElasticIndex(final ElasticsearchPersistentEntity documentMetadata, final List<?> documentsToStore) {

        // Get mapping classes from the documents to store
        Set<Class<?>> mappingClasses =
                documentsToStore
                        .stream()
                        .map(Object::getClass)
                        .distinct()
                        .collect(Collectors.toSet());

        // Define type mappings in the Elasticsearch indices
        for (Class<?> mappingClass : mappingClasses) {

            // Clean data if necessary
            if (esOperations.indexExists(mappingClass)) {
                DeleteQuery deleteQuery = new DeleteQuery();
                deleteQuery.setQuery(QueryBuilders.matchAllQuery());
                esOperations.delete(deleteQuery, mappingClass);
            }
            // Create index for the mapping class if it doesn't already exist
            else
                esOperations.createIndex(mappingClass);

            // Define mapping for the document type
            esOperations.putMapping(mappingClass);
        }

        // Bulk index documents to store
        List<IndexQuery> indexQueries = new ArrayList<>();
        for (Object documentToStore : documentsToStore) {
            final ElasticsearchPersistentEntity documentToStoreMetadata = DocumentMetaDataBuilder.buildDocumentMetadata(esOperations, documentToStore.getClass());
            IndexQueryBuilder indexQueryBuilder = new IndexQueryBuilder();
            try {
                indexQueryBuilder.withId(String.class.cast(documentToStoreMetadata.getIdProperty().getField().get(documentToStore)));
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            indexQueryBuilder.withObject(documentToStore);

            indexQueries.add(indexQueryBuilder.build());
        }
        esOperations.bulkIndex(indexQueries);

        // Refresh indices for data to be searchable
        for (Class<?> mappingClass : mappingClasses)
            esOperations.refresh(mappingClass, true);

        // Number of loaded documents
        loadedDocumentCount = indexQueries.stream().filter(indexQuery -> indexQuery.getObject().getClass().equals(documentMetadata.getType())).count();
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
}
