package info.jallaix.spring.data.es.test;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Test class for the Spring Data Elasticsearch module.<br/>
 * It supports data initialization thanks to the <a href="https://github.com/tlrx/elasticsearch-test">elasticsearch-test framework</a>.<br/>
 */
public abstract class SpringDataEsTestCase<T, ID extends Serializable, R extends ElasticsearchRepository<T, ID>> {

    /**
     * Test documents loader
     */
    @Autowired
    private TestDocumentsLoader testDocumentsLoader;

    /**
     * Logger
     */
    private static final Logger logger = LoggerFactory.getLogger(SpringDataEsTestCase.class);


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                           Pre-test data loading                                                */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Create an Elasticsearch index and type and load custom data in it.
     */
    @Before
    public void initElasticIndex() {

        documentClass = findDocumentClass();                            // Find document class
        documentMetadata = findDocumentMetadata(documentClass);         // Find document metadata (index, type)
        documentIdField = findIdFieldForDocument(documentClass);  // Find document identifier
        testDocumentsLoader.initElasticIndex(                           // Load documents into index
                documentMetadata.indexName(),
                documentMetadata.type(),
                this.getClass().getName());
    }

    /**
     * Free resources used by Elastic.
     */
    @After
    public void terminateElasticIndex() { testDocumentsLoader.terminateElasticIndex(); }

    /**
     * Get the test documents loader
     * @return The test documents loader
     */
    protected TestDocumentsLoader getTestDocumentsLoader() { return testDocumentsLoader; }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                           Tested document metadata                                             */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Elastic document metadata
     */
    protected Document documentMetadata;

    /**
     * Class of the document
     */
    protected Class<T> documentClass;

    /**
     * Document identifier
     */
    protected Field documentIdField;

    /**
     * Find Elastic metadata (index, type, ...) of the tested document
     * @return The document metadata
     */
    private Document findDocumentMetadata(Class<T> documentClass) {

        // Get annotation from document class
        return documentClass.getDeclaredAnnotation(Document.class);
    }

    /**
     * Find the class of the tested document
     * @return The document class
     */
    private Class<T> findDocumentClass() {

        // Find document class
        ParameterizedType superClass = (ParameterizedType) getClass().getGenericSuperclass();
        Type[] types = superClass.getActualTypeArguments();
        @SuppressWarnings("unchecked")
        Class<T> documentClass = (Class<T>) types[0];

        return documentClass;
    }

    /**
     * Find the the field used as the key of a document
     * @param documentClass The document class
     * @return The found field
     */
    private Field findIdFieldForDocument(Class<T> documentClass) {

        // Find field in document class with "Id" annotation
        for (Field field : documentClass.getDeclaredFields()) {
            if (field.getDeclaredAnnotation(Id.class) != null) {
                field.setAccessible(true);
                return field;
            }
        }

        return null;
    }

    /**
     * Get the Elastic document metadata
     * @return The Elastic document metadata
     */
    @SuppressWarnings("unused")
    protected Document getDocumentMetadata() { return documentMetadata; }

    /**
     * Get the identifier value of a document
     * @param document The document
     * @return The found identifier value
     */
    protected ID getIdFieldValue(T document) {

        try {
            @SuppressWarnings("unchecked")
            ID id = (ID)documentIdField.get(document);
            return id;
        } catch (IllegalAccessException e) {
            logger.error(null, e);
            return null;
        }
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                              Tested Repository                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Tested repository
     */
    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    protected R repository;

    /**
     * Give access to the tested repository.
     * @return The tested repository
     */
    @SuppressWarnings("unused")
    protected R getRepository() { return repository; }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                               Abstract methods                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Return a new document for insertion.
     * @return A document that will be inserted
     */
    protected abstract T newDocumentToInsert();

    /**
     * Return a new document for update.
     * @return A document that will update an existing one
     */
    protected abstract T newDocumentToUpdate();

    /**
     * Return the size of a page to get
     * @return The size of a page to get
     */
    protected abstract int getPageSize();
}
