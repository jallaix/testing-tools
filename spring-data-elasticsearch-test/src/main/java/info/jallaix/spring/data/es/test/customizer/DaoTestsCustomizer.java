package info.jallaix.spring.data.es.test.customizer;

import info.jallaix.spring.data.es.test.testcase.BaseDaoElasticsearchTestCase;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;

import java.util.List;

/**
 * <p>Declare methods used to customize default Elasticsearch repository tests.</p>
 * <p>Any implementation is provided by using the {@link BaseDaoElasticsearchTestCase#setCustomizer(DaoTestsCustomizer)} method.</p>
 */
@SuppressWarnings("unused")
public interface DaoTestsCustomizer<T> {

    /**
     * Add additional content to the {@link BaseDaoElasticsearchTestCase#saveNewDocument()} and {@link BaseDaoElasticsearchTestCase#indexNewDocument()} tests.
     *
     * @param toInsert Document to insert
     * @param inserted Inserted document
     */
    void customizeSaveNewDocument(T toInsert, T inserted);

    /**
     * Get custom data before saving to make it available to the {@link #customizeSaveExistingDocument(Object, Object, Object)} method
     *
     * @param toUpdate Document to update
     * @return The custom data
     */
    Object getCustomDataOnSaveExistingDocument(T toUpdate);

    /**
     * Add additional content to the {@link BaseDaoElasticsearchTestCase#saveExistingDocument()} and {@link BaseDaoElasticsearchTestCase#indexExistingDocument()} tests.
     *
     * @param toUpdate   Document to update
     * @param updated    Updated document
     * @param customData Custom data
     */
    void customizeSaveExistingDocument(T toUpdate, T updated, Object customData);

    /**
     * Get custom data before saving to make it available to the {@link #customizeSaveDocuments(List, List, Object)} method
     *
     * @param toSave Documents to save
     * @return The custom data
     */
    Object getCustomDataOnSaveDocuments(List<T> toSave);

    /**
     * Add additional content to the {@link BaseDaoElasticsearchTestCase#saveDocuments()} test.
     *
     * @param toSave Documents to save
     * @param saved  Saved documents
     */
    void customizeSaveDocuments(List<T> toSave, List<T> saved, Object customData);

    /**
     * Customize a list of typed documents used as fixture in the {@link BaseDaoElasticsearchTestCase#findAllDocuments()} and so on tests.
     *
     * @param fixture The list of typed documents to customize
     * @return The list of customized typed documents
     */
    List<T> customizeFindAllFixture(final List<T> fixture);

    /**
     * Customize a typed document used as fixture in the {@link BaseDaoElasticsearchTestCase#findOneExistingDocument()} test.
     *
     * @param fixture The typed document to customize
     * @return The customized typed document
     */
    T customizeFindOneFixture(final T fixture);

    /**
     * Add additional content to the {@link BaseDaoElasticsearchTestCase#deleteOneExistingDocument()} and {@link BaseDaoElasticsearchTestCase#deleteOneExistingDocumentById()} tests.
     *
     * @param id Identifier of the deleted document
     */
    void customizeDeleteOne(String id);

    /**
     * Add additional content to the {@link BaseDaoElasticsearchTestCase#deleteAllDocuments()} test.
     */
    void customizeDeleteAll();

    /**
     * Add additional content to the {@link BaseDaoElasticsearchTestCase#deleteExistingDocumentSet()} test.
     *
     * @param toDelete List of documents to delete
     */
    void customizeDeleteSet(List<T> toDelete);

    /**
     * Update an HTTP entity before it's sent to the server.
     *
     * @param httpEntity The original HTTP entity
     * @return The updated HTTP entity
     */
    HttpEntity<?> customizeHttpEntity(HttpEntity<?> httpEntity);

    /**
     * Add additional assertions to an HTTP response.
     *
     * @param response The HTTP response
     */
    void assertResponse(ResponseEntity<?> response);
}
