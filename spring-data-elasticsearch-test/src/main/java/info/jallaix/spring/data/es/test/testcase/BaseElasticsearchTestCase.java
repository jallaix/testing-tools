package info.jallaix.spring.data.es.test.testcase;

import info.jallaix.spring.data.es.test.util.TestDocumentsLoader;
import info.jallaix.spring.data.es.test.util.DocumentMetaDataUtil;
import lombok.AccessLevel;
import lombok.Getter;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Set;

/**
 * <p>
 * Test class for the Spring Data Elasticsearch module.
 * <p>
 * It supports data initialization thanks to the <a href="https://github.com/tlrx/elasticsearch-test">elasticsearch-test framework</a>.
 */
public abstract class BaseElasticsearchTestCase<T, ID extends Serializable, R extends ElasticsearchRepository<T, ID>> {

    /**
     * Ability to get the current test name
     */
    @Rule
    public TestName name = new TestName();

    /**
     * Set of tested methods
     */
    protected Set<Class<?>> testedMethods;

    /**
     * Tested repository
     */
    @Autowired
    @Getter(AccessLevel.PROTECTED)
    private R repository;

    /**
     * This object loads some data for testing in an Elasticsearch index
     */
    @Autowired
    private TestDocumentsLoader testDocumentsLoader;

    /**
     * Elastic document metadata
     */
    @Getter(AccessLevel.PROTECTED)
    private Document documentMetadata;

    /**
     * Class of the document
     */
    @Getter(AccessLevel.PROTECTED)
    private Class<T> documentClass;

    /**
     * Document identifier
     */
    @Getter(AccessLevel.PROTECTED)
    private Field documentIdField;


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
     * Return a new existing document.
     * @return A document that exists
     */
    protected abstract T newExistingDocument();

    /**
     * Return the sort field
     * @return The sort field
     */
    protected abstract Field getSortField();

    /**
     * Return the size of a page to get
     * @return The size of a page to get
     */
    protected abstract int getPageSize();


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                                Data loading                                                    */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Create an Elasticsearch index and type and load custom data in it.
     */
    @Before
    public void initElasticIndex() {

        initDocumentClass();                                            // Find document class
        DocumentMetaDataUtil<T> documentMetaDataUtil = new DocumentMetaDataUtil<>(documentClass);
        documentMetadata = documentMetaDataUtil.getDocumentMetadata();  // Find document metadata (index, type)
        documentIdField = documentMetaDataUtil.getIdFieldForDocument(); // Find document identifier
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
    @SuppressWarnings("unused")
    protected TestDocumentsLoader getTestDocumentsLoader() { return testDocumentsLoader; }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                           Tested document metadata                                             */
    /*----------------------------------------------------------------------------------------------------------------*/

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
            throw new RuntimeException(e);
        }
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                            Configurable test system                                            */
    /*----------------------------------------------------------------------------------------------------------------*/

    @Before
    public void selectTests() {
        Assume.assumeTrue(isTestPlayed(testedMethods));
    }

    /**
     * Determine if a test is played
     *
     * @return {@code true} if the test has to be played else {@code false}
     */
    protected boolean isTestPlayed(Set<Class<?>> testedMethods) {

        try {
            // Find the test category
            Category category = this.getClass()
                    .getMethod(name.getMethodName())
                    .getAnnotation(Category.class);

            if (category == null)
                return true;

            // Find if one of the category classes belongs to the methods to be tested
            return Arrays.asList(category.value())
                    .stream()
                    .anyMatch(testedMethods::contains);

        } catch (NoSuchMethodException e) {
            return true;
        }
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                                Private methods                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Initialize the document class
     */
    private void initDocumentClass() {

        // Find document class
        final ParameterizedType superClass = (ParameterizedType) getClass().getGenericSuperclass();
        final Type[] types = superClass.getActualTypeArguments();
        //noinspection unchecked
        documentClass = (Class<T>) types[0];
    }
}
