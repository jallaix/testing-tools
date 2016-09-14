package info.jallaix.spring.data.es.test.testcase;

import info.jallaix.spring.data.es.test.bean.DocumentMetaData;
import info.jallaix.spring.data.es.test.util.DocumentMetaDataBuilder;
import info.jallaix.spring.data.es.test.util.TestDocumentsLoader;
import lombok.AccessLevel;
import lombok.Getter;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Set;

/**
 * <p/>
 * Test class for the Spring Data Elasticsearch module.
 * <p/>
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
    @SuppressWarnings("SpringJavaAutowiringInspection")
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
    private DocumentMetaData<T> documentMetaData;


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                               Tests life cycle                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    @Before
    public void init() {

        selectTests();              // Exit if the test is not configured to be played
        feedElasticIndex();         // Fixture data for testing
    }

    @After
    public void exit() {
        terminateElasticIndex();    // Free fixture data
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                               Abstract methods                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Return a new document for insertion.
     *
     * @return A document that will be inserted
     */
    protected abstract T newDocumentToInsert();

    /**
     * Return a new document for update.
     *
     * @return A document that will update an existing one
     */
    protected abstract T newDocumentToUpdate();

    /**
     * Return a new existing document.
     *
     * @return A document that exists
     */
    protected abstract T newExistingDocument();

    /**
     * Return the sort field
     *
     * @return The sort field
     */
    protected abstract Field getSortField();

    /**
     * Return the size of a page to get
     *
     * @return The size of a page to get
     */
    protected abstract int getPageSize();


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                                Data loading                                                    */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Create an Elasticsearch index and type and load custom data in it.
     */
    public void feedElasticIndex() {

        // Initialize document metadata
        documentMetaData = DocumentMetaDataBuilder.buildDocumentMetadata(initDocumentClass());

        // Load documents into index
        testDocumentsLoader.initElasticIndex(
                documentMetaData.getDocumentAnnotation().indexName(),
                documentMetaData.getDocumentAnnotation().type(),
                this.getClass().getName());
    }

    /**
     * Free resources used by Elastic.
     */
    public void terminateElasticIndex() {
        testDocumentsLoader.terminateElasticIndex();
    }

    /**
     * Get the test documents loader
     *
     * @return The test documents loader
     */
    @SuppressWarnings("unused")
    protected TestDocumentsLoader getTestDocumentsLoader() {
        return testDocumentsLoader;
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                           Tested document metadata                                             */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Get the identifier value of a document
     *
     * @param document The document
     * @return The found identifier value
     */
    protected ID getIdFieldValue(T document) {

        try {
            @SuppressWarnings("unchecked")
            ID id = (ID) documentMetaData.getDocumentIdField().get(document);
            return id;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                            Configurable test system                                            */
    /*----------------------------------------------------------------------------------------------------------------*/

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
    private Class<T> initDocumentClass() {

        // Find document class
        final ParameterizedType superClass = (ParameterizedType) getClass().getGenericSuperclass();
        final Type[] types = superClass.getActualTypeArguments();

        //noinspection unchecked
        return (Class<T>) types[0];
    }
}
