package info.jallaix.spring.data.es.test.bean;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Created by JAX on 10/03/2017.
 */
public interface BaseElasticsearchTestFixture<T> {
    /**
     * Return a new document for insertion.
     *
     * @return A document that will be inserted
     */
    T newDocumentToInsert();

    /**
     * Return a new document for update.
     *
     * @return A document that will update an existing one
     */
    T newDocumentToUpdate();

    /**
     * Return a new existing document.
     *
     * @return A document that exists
     */
    T newExistingDocument();

    /**
     * Return the sort field
     *
     * @return The sort field
     */
    Field getSortField();

    /**
     * Return the size of a page to get
     *
     * @return The size of a page to get
     */
    int getPageSize();

    /**
     * Return the list of document to store in the index before each test
     * @return The list of document to store in the index
     */
    List<?> getStoredDocuments();
}
