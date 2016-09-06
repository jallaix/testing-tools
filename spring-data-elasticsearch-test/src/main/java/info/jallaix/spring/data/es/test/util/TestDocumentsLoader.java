package info.jallaix.spring.data.es.test.util;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.github.tlrx.elasticsearch.test.EsSetupRuntimeException;
import com.github.tlrx.elasticsearch.test.provider.JSONProvider;
import com.github.tlrx.elasticsearch.test.request.CreateIndex;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.tlrx.elasticsearch.test.EsSetup.deleteAll;
import static com.github.tlrx.elasticsearch.test.EsSetup.fromClassPath;

/**
 * This class is used to load documents in an Elasticsearch index.
 * Unit tests may perform index operations against these data.
 */
public class TestDocumentsLoader {

    /**
     * File extension for document mapping
     */
    protected static final String DOCUMENT_MAPPING_EXTENSION = ".mapping.json";
    /**
     * File extension for document data
     */
    protected static final String DOCUMENT_DATA_EXTENSION = ".data.bulk";

    /**
     * Elasticsearch client
     */
    private Client esClient;
    /**
     * Elasticsearch setup for index initialization
     */
    private EsSetup esSetup;

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
     * Constructor with Elasticsearch client
     * @param esClient the Elasticsearch client
     */
    public TestDocumentsLoader(Client esClient) { this.esClient = esClient; }

    /**
     * Create an Elasticsearch index with sample documents
     * @param indexName Index name
     * @param documentType Document type
     * @param testedClassName Tested class name
     */
    public void initElasticIndex(String indexName, String documentType, String testedClassName) {

        CreateIndex index = initIndexWithMapping(                                   // Apply mapping to document type
                indexName,
                documentType,
                testedClassName);
        esSetup = setupIndexWithData(index, testedClassName);                      // Load documents in the index
        if (esSetup != null)
            loadedDocumentCount = countLoadedDocuments(indexName, documentType);    // Number of loaded documents
    }

    /**
     * Free resources used by Elasticsearch
     */
    public void terminateElasticIndex() {

        if (esSetup != null)
            esSetup.terminate();
    }

    /**
     * Get the number of loaded documents
     * @return The number of loaded documents
     */
    public long getLoadedDocumentCount() { return loadedDocumentCount; }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                              Private methods                                                   */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Create an Elasticsearch index with a document type
     * @param indexName Index name
     * @param documentType Document testedClassName
     * @param testedClassName Tested class name
     * @return Index creation data
     */
    private CreateIndex initIndexWithMapping(String indexName, String documentType, String testedClassName) {

        CreateIndex createIndex = EsSetup.createIndex(indexName);

        // Add mapping to the Elastic documentType
        JSONProvider mappingClassPath = fromClassPath(testedClassName.replace(".", "/") + DOCUMENT_MAPPING_EXTENSION);
        try {
            createIndex.withMapping(documentType, mappingClassPath);
        }
        catch (EsSetupRuntimeException e) {
            //noinspection ThrowableResultOfMethodCallIgnored
            logger.warn(ExceptionUtils.getRootCause(e).getMessage());
        }

        return createIndex;
    }

    /**
     * Load data in an Elasticsearch index before a test executes
     * @param createIndex Index creation data
     * @param testedClassName Tested class name
     * @return An Elasticsearch setup
     */
    private EsSetup setupIndexWithData(CreateIndex createIndex, String testedClassName) {

        // Add data to the Elasticsearch index
        JSONProvider dataClassPath = fromClassPath(testedClassName.replace(".", "/") + DOCUMENT_DATA_EXTENSION);
        createIndex.withData(dataClassPath);

        // Setup Elasticsearch index initialization
        EsSetup esSetup = new EsSetup(esClient, false);
        try {
            esSetup.execute(deleteAll(), createIndex);
        }
        catch (EsSetupRuntimeException e) {
            //noinspection ThrowableResultOfMethodCallIgnored
            logger.warn(ExceptionUtils.getRootCause(e).getMessage());
            esSetup = null;
        }

        return esSetup;
    }

    /**
     * Count the number of documents loaded in the index
     * @param indexName Index name
     * @param documentType Document type
     */
    private long countLoadedDocuments(String indexName, String documentType) {
        return esClient.prepareCount(indexName).setTypes(documentType).get().getCount();
    }
}
