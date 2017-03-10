package info.jallaix.spring.data.es.test.customizer;

import info.jallaix.spring.data.es.test.testcase.BaseDaoElasticsearchTestCase;

import java.util.List;

/**
 * Base implementation for the {@link DaoTestsCustomizer} interface.
 */
public class BaseDaoTestsCustomizer<T> implements DaoTestsCustomizer<T> {

    /**
     * Add additional content to the {@link BaseDaoElasticsearchTestCase#saveNewDocument()} and {@link BaseDaoElasticsearchTestCase#indexNewDocument()} tests.
     *
     * @param toInsert Document to insert
     * @param inserted Inserted document
     */
    @Override
    public void customizeSaveNewDocument(T toInsert, T inserted) {
    }

    /**
     * Get custom data before saving to make it available to the {@link #customizeSaveExistingDocument(Object, Object, Object)} method
     *
     * @param toUpdate Document to update
     * @return The custom data
     */
    @Override
    public Object getCustomDataOnSaveExistingDocument(T toUpdate) {
        return null;
    }

    /**
     * Add additional content to the {@link BaseDaoElasticsearchTestCase#saveExistingDocument()} and {@link BaseDaoElasticsearchTestCase#indexExistingDocument()} tests.
     *
     * @param toUpdate   Document to update
     * @param updated    Updated document
     * @param customData Custom data
     */
    @Override
    public void customizeSaveExistingDocument(T toUpdate, T updated, Object customData) {
    }

    /**
     * Get custom data before saving to make it available to the {@link #customizeSaveDocuments(List, List, Object)} method
     *
     * @param toSave Documents to save
     * @return The custom data
     */
    @Override
    public Object getCustomDataOnSaveDocuments(List<T> toSave) {
        return null;
    }

    /**
     * Add additional content to the {@link BaseDaoElasticsearchTestCase#saveDocuments()} test.
     *
     * @param toSave     Documents to save
     * @param saved      Saved documents
     * @param customData Custom data
     */
    @Override
    public void customizeSaveDocuments(List<T> toSave, List<T> saved, Object customData) {
    }

    /**
     * Customize a list of typed documents used as fixture in the {@link BaseDaoElasticsearchTestCase#findAllDocuments()} and so on tests.
     *
     * @param fixture The list of typed documents to customize
     * @return The list of customized typed documents
     */
    @Override
    public List<T> customizeFindAllFixture(List<T> fixture) {
        return null;
    }

    /**
     * Customize a typed document used as fixture in the {@link BaseDaoElasticsearchTestCase#findOneExistingDocument()} test.
     *
     * @param fixture The typed document to customize
     * @return The customized typed document
     */
    @Override
    public T customizeFindOneFixture(T fixture) {
        return null;
    }

    /**
     * Add additional content to the {@link BaseDaoElasticsearchTestCase#deleteOneExistingDocument()} and {@link BaseDaoElasticsearchTestCase#deleteOneExistingDocumentById()} tests.
     *
     * @param id Identifier of the deleted document
     */
    @Override
    public void customizeDeleteOne(String id) {
    }

    /**
     * Add additional content to the {@link BaseDaoElasticsearchTestCase#deleteAllDocuments()} test.
     */
    @Override
    public void customizeDeleteAll() {
    }

    /**
     * Add additional content to the {@link BaseDaoElasticsearchTestCase#deleteExistingDocumentSet()} test.
     *
     * @param toDelete List of documents to delete
     */
    @Override
    public void customizeDeleteSet(List<T> toDelete) {
    }
}
