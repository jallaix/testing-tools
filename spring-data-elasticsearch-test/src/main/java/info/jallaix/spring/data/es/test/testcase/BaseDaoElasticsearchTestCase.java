package info.jallaix.spring.data.es.test.testcase;

import info.jallaix.spring.data.es.test.customizer.BaseDaoTestsCustomizer;
import info.jallaix.spring.data.es.test.customizer.DaoTestsCustomizer;
import info.jallaix.spring.data.es.test.util.TestClientOperations;
import info.jallaix.spring.data.es.test.util.TestDocumentsLoader;
import org.elasticsearch.action.ActionRequestValidationException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * <p/>
 * Test class for the Spring Data Elasticsearch module.<br>
 * It supports data initialization thanks to the <a href="https://github.com/tlrx/elasticsearch-test">elasticsearch-test framework</a>.<br>
 * It also performs generic CRUD tests on the tested repository.<br>
 * <p/>
 * <p/>
 * The repository must verify the following tests related to document <b>indexing</b> or <b>saving</b> (same behavior) :
 * <ul>
 * <li>Indexing a null document throws an IllegalArgumentException.</li>
 * <li>Saving a null document throws an IllegalArgumentException.</li>
 * <li>Indexing a new document inserts the document in the index.</li>
 * <li>Saving a new document inserts the document in the index.</li>
 * <li>Indexing an existing document replaces the document in the index.</li>
 * <li>Saving an existing document replaces the document in the index.</li>
 * <li>Saving a list of documents with one null throws an IllegalArgumentException and no document is indexed.</li>
 * <li>Saving a list of documents inserts and updates the documents in the index.</li>
 * </ul>
 * <p/>
 * The repository must verify the following tests related to document <b>finding</b> :
 * <ul>
 * <li>Finding a list of all existing documents returns an iterable with all these documents.</li>
 * <li>Finding a list of existing documents by identifier returns an iterable with all these documents.</li>
 * <li>Finding a sorted list of all existing documents returns an iterable with all these documents sorted.</li>
 * <li>Finding a page of existing documents returns an iterable with all these documents for a page.</li>
 * <li>Finding a sorted page of existing documents returns an iterable with all these documents sorted for a page.</li>
 * <li>Finding a document with a null identifier throws an ActionRequestValidationException.</li>
 * <li>Finding a document that doesn't exist returns a null document.</li>
 * <li>Finding a document that exists returns this document.</li>
 * <li>Testing the existence of a document with a null identifier throws an ActionRequestValidationException.</li>
 * <li>Testing the existence of a document that doesn't exist returns false.</li>
 * <li>Testing the existence of a document that exists returns true.</li>
 * <li>Counting the number of documents returns the number of documents in the index type</li>
 * </ul>
 * <p/>
 * The repository must verify the following tests related to document <b>deleting</b> :
 * <ul>
 * <li>Deleting all documents leaves an empty index type.</li>
 * <li>Deleting a missing document set doesn't remove these documents from the index type.</li>
 * <li>Deleting an existing document set removes these documents from the index type.</li>
 * <li>Deleting a missing document set doesn't remove this document from the index type.</li>
 * <li>Deleting an existing document set removes this document from the index type.</li>
 * <li>Deleting a missing document by identifier set doesn't remove this document from the index type.</li>
 * <li>Deleting an existing document by identifier set removes this document from the index type.</li>
 * </ul>
 */
public abstract class BaseDaoElasticsearchTestCase<T, ID extends Serializable, R extends ElasticsearchRepository<T, ID>> extends BaseElasticsearchTestCase<T, ID, R> {

    /**
     * Test documents loader
     */
    @Autowired
    protected TestDocumentsLoader testDocumentsLoader;

    /**
     * Test client operations
     */
    @Autowired
    protected TestClientOperations testClientOperations;

    /**
     * Tests customizer
     */
    private DaoTestsCustomizer<T> customizer;


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                           Ignored tests system                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Constructor with list of methods to test
     *
     * @param methods Methods to test
     */
    @SafeVarargs
    public BaseDaoElasticsearchTestCase(Class<? extends DaoTestedMethod>... methods) {

        if (methods.length == 0)
            testedMethods = new HashSet<>(Arrays.asList(
                    DaoTestedMethod.Index.class,
                    DaoTestedMethod.Index.class,
                    DaoTestedMethod.Save.class,
                    DaoTestedMethod.SaveBulk.class,
                    DaoTestedMethod.FindAll.class,
                    DaoTestedMethod.FindAllById.class,
                    DaoTestedMethod.FindAllPageable.class,
                    DaoTestedMethod.FindAllSorted.class,
                    DaoTestedMethod.FindOne.class,
                    DaoTestedMethod.Exist.class,
                    DaoTestedMethod.Count.class,
                    DaoTestedMethod.DeleteAll.class,
                    DaoTestedMethod.DeleteAllById.class,
                    DaoTestedMethod.Delete.class,
                    DaoTestedMethod.DeleteById.class));
        else
            testedMethods = new HashSet<>(Arrays.asList(methods));
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                            Tests customization                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Set a tests customizer. A {@link BaseDaoTestsCustomizer} is defined if {@code null} is provided.
     *
     * @param customizer The customizer to set
     */
    @SuppressWarnings("unused")
    public void setCustomizer(DaoTestsCustomizer<T> customizer) {
        if (customizer == null)
            this.customizer = new BaseDaoTestsCustomizer<>();
        else
            this.customizer = customizer;
    }

    /**
     * Get a tests customizer. A {@link BaseDaoTestsCustomizer} is defined if none already exists.
     *
     * @return The customizer found
     */
    private DaoTestsCustomizer<T> getCustomizer() {
        if (this.customizer == null)
            this.customizer = new BaseDaoTestsCustomizer<>();

        return this.customizer;
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                    Tests related to document indexing                                          */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Indexing a null document throws an IllegalArgumentException.
     */
    @Category(DaoTestedMethod.Index.class)
    @Test(expected = IllegalArgumentException.class)
    public void indexNullDocument() {
        getRepository().index(null);
    }

    /**
     * Saving a null document throws an IllegalArgumentException.
     */
    @Category(DaoTestedMethod.Save.class)
    @Test(expected = IllegalArgumentException.class)
    public void saveNullDocument() {
        getRepository().save((T) null);
    }

    /**
     * Indexing a new document inserts the document in the index.
     */
    @Category(DaoTestedMethod.Index.class)
    @Test
    public void indexNewDocument() {

        T toInsert = getTestFixture().newDocumentToInsert();
        T inserted = getRepository().index(toInsert);

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount() + 1,
                testClientOperations.countDocuments(getDocumentMetadata()));
        assertEquals(toInsert, inserted);

        // Customizable test function
        getCustomizer().customizeSaveNewDocument(toInsert, inserted);
    }

    /**
     * Saving a new document inserts the document in the index.
     */
    @Category(DaoTestedMethod.Save.class)
    @Test
    public void saveNewDocument() {

        T toInsert = getTestFixture().newDocumentToInsert();
        T inserted = getRepository().save(toInsert);

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount() + 1,
                testClientOperations.countDocuments(getDocumentMetadata()));
        assertEquals(toInsert, inserted);

        // Customizable test function
        getCustomizer().customizeSaveNewDocument(toInsert, inserted);
    }

    /**
     * Indexing an existing document replaces the document in the index.
     */
    @Category(DaoTestedMethod.Index.class)
    @Test
    public void indexExistingDocument() {

        T toUpdate = getTestFixture().newDocumentToUpdate();

        // Get custom data before saving to make it available to customizeSaveExistingDocument()
        Object customData = getCustomizer().getCustomDataOnSaveExistingDocument(toUpdate);

        T updated = getRepository().index(toUpdate);

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount(),
                testClientOperations.countDocuments(getDocumentMetadata()));
        assertEquals(toUpdate, updated);

        // Customizable test function
        getCustomizer().customizeSaveExistingDocument(toUpdate, updated, customData);
    }

    /**
     * Saving an existing document replaces the document in the index.
     */
    @Category(DaoTestedMethod.Save.class)
    @Test
    public void saveExistingDocument() {

        T toUpdate = getTestFixture().newDocumentToUpdate();

        // Get custom data before saving to make it available to customizeSaveExistingDocument()
        Object customData = getCustomizer().getCustomDataOnSaveExistingDocument(toUpdate);

        T updated = getRepository().save(toUpdate);

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount(),
                testClientOperations.countDocuments(getDocumentMetadata()));
        assertEquals(toUpdate, updated);

        // Customizable test function
        getCustomizer().customizeSaveExistingDocument(toUpdate, updated, customData);
    }

    /**
     * Indexing a list of documents with one null throws an IllegalArgumentException and no document is indexed.
     */
    @Category(DaoTestedMethod.SaveBulk.class)
    @Test
    public void saveNullDocuments() {

        try {
            getRepository().save(Arrays.asList(getTestFixture().newDocumentToInsert(), getTestFixture().newDocumentToUpdate(), null));
            fail("IllegalArgumentException must be thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(
                    testDocumentsLoader.getLoadedDocumentCount(),
                    testClientOperations.countDocuments(getDocumentMetadata()));
        }
    }

    /**
     * Saving a list of documents inserts and updates the documents in the index.
     */
    @Category(DaoTestedMethod.Save.class)
    @Test
    public void saveDocuments() {

        List<T> toSave = Arrays.asList(getTestFixture().newDocumentToInsert(), getTestFixture().newDocumentToUpdate());

        // Get custom data before saving to make it available to customizeSaveDocuments()
        Object customData = getCustomizer().getCustomDataOnSaveDocuments(toSave);

        List<T> saved = new ArrayList<>(2);
        getRepository().save(toSave).forEach(saved::add);

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount() + 1,
                testClientOperations.countDocuments(getDocumentMetadata()));
        assertArrayEquals(toSave.toArray(), saved.toArray());

        // Customizable test function
        getCustomizer().customizeSaveDocuments(toSave, saved, customData);
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                     Tests related to document finding                                          */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Finding a list of all existing documents returns an iterable with all these documents.
     */
    @Category(DaoTestedMethod.FindAll.class)
    @Test
    public void findAllDocuments() {

        // Get all typed documents from the index
        List<T> initialList = getCustomizer().customizeFindAllFixture(testClientOperations.findAllDocumentsPaged(
                getDocumentMetadata(),
                0,
                (int) this.getTestDocumentsLoader().getLoadedDocumentCount()));

        // Repository search
        List<T> foundList = new ArrayList<>();
        getRepository().findAll()
                .forEach(foundList::add);

        assertArrayEquals(initialList.toArray(), foundList.toArray());
    }

    /**
     * Finding a list of existing documents by identifier returns an iterable with all these documents.
     */
    @Category(DaoTestedMethod.FindAllById.class)
    @Test
    public void findAllDocumentsByIdentifier() {

        // Get some typed documents from the index depending of the default page size
        List<T> initialList = getCustomizer().customizeFindAllFixture(testClientOperations.findAllDocuments(getDocumentMetadata()));
        List<ID> initialKeys = initialList.stream()
                .map(this::getIdFieldValue)
                .collect(Collectors.toList());

        // Repository search
        List<T> foundList = new ArrayList<>();
        getRepository().findAll(initialKeys)
                .forEach(foundList::add);

        assertArrayEquals(initialList.toArray(), foundList.toArray());
    }

    /**
     * Finding a sorted list of all existing documents returns an iterable with all these documents sorted.
     */
    @Category(DaoTestedMethod.FindAllSorted.class)
    @Test
    public void findAllDocumentsSorted() {

        // Get all typed documents sorted from the index
        List<T> initialList = getCustomizer().customizeFindAllFixture(testClientOperations.findAllDocumentsPagedSorted(
                getDocumentMetadata(),
                getTestFixture().getSortField(),
                0,
                (int) this.getTestDocumentsLoader().getLoadedDocumentCount()));

        // Repository search
        Sort sorting = new Sort(Sort.Direction.DESC, getTestFixture().getSortField().getName());
        List<T> foundList = new ArrayList<>();
        getRepository().findAll(sorting)
                .forEach(foundList::add);

        assertArrayEquals(initialList.toArray(), foundList.toArray());
    }

    /**
     * Finding a page of existing documents returns an iterable with all these documents for a page.
     */
    @Category(DaoTestedMethod.FindAllPageable.class)
    @Test
    public void findAllDocumentsByPage() {

        // Define the page parameters
        long documentsCount = testClientOperations.countDocuments(getDocumentMetadata());
        Assert.isTrue(documentsCount > 0, "No document loaded");
        int pageSize = getTestFixture().getPageSize();
        Assert.isTrue(pageSize > 0, "Page size must be positive");
        int nbPages = (int) documentsCount / pageSize + (documentsCount % pageSize == 0 ? 0 : 1);

        // Get typed documents from the index for the first page
        List<T> initialList = getCustomizer().customizeFindAllFixture(testClientOperations.findAllDocumentsPaged(
                getDocumentMetadata(),
                0,
                getTestFixture().getPageSize()));

        // Repository search
        List<T> foundList = new ArrayList<>();
        getRepository().findAll(new PageRequest(0, pageSize))
                .forEach(foundList::add);

        assertArrayEquals(initialList.toArray(), foundList.toArray());

        // Get typed documents from the index for the last page
        initialList = getCustomizer().customizeFindAllFixture(testClientOperations.findAllDocumentsPaged(
                getDocumentMetadata(),
                nbPages - 1,
                getTestFixture().getPageSize()));

        // Repository search
        foundList.clear();
        getRepository().findAll(new PageRequest(nbPages - 1, pageSize))
                .forEach(foundList::add);

        assertArrayEquals(initialList.toArray(), foundList.toArray());
    }

    /**
     * Finding a sorted page of existing documents returns an iterable with all these documents sorted for a page.
     */
    @Category(DaoTestedMethod.FindAllPageable.class)
    @Test
    public void findAllDocumentsByPageSorted() {

        // Define the page parameters
        long documentsCount = testClientOperations.countDocuments(getDocumentMetadata());
        Assert.isTrue(documentsCount > 0, "No document loaded");
        int pageSize = getTestFixture().getPageSize();
        Assert.isTrue(pageSize > 0, "Page size must be positive");
        int nbPages = (int) documentsCount / pageSize + (documentsCount % pageSize == 0 ? 0 : 1);

        // Fixture for first page
        List<T> initialList = getCustomizer().customizeFindAllFixture(testClientOperations.findAllDocumentsPagedSorted(
                getDocumentMetadata(),
                getTestFixture().getSortField(),
                0,
                getTestFixture().getPageSize()));

        // Repository search
        Sort sorting = new Sort(Sort.Direction.DESC, getTestFixture().getSortField().getName());
        List<T> foundList = new ArrayList<>();
        getRepository().findAll(new PageRequest(0, pageSize, sorting))
                .forEach(foundList::add);

        assertArrayEquals(initialList.toArray(), foundList.toArray());

        // Fixture for last page
        initialList = getCustomizer().customizeFindAllFixture(testClientOperations.findAllDocumentsPagedSorted(
                getDocumentMetadata(),
                getTestFixture().getSortField(),
                nbPages - 1,
                pageSize));

        // Repository search
        foundList.clear();
        getRepository().findAll(new PageRequest(nbPages - 1, pageSize, sorting))
                .forEach(foundList::add);

        assertArrayEquals(initialList.toArray(), foundList.toArray());
    }

    /**
     * Finding a document with a null identifier throws an ActionRequestValidationException.
     */
    @Category(DaoTestedMethod.FindOne.class)
    @Test(expected = ActionRequestValidationException.class)
    public void findOneNullDocument() {

        getRepository().findOne(null);

        fail("Should thrown an ActionRequestValidationException");
    }

    /**
     * Finding a document that doesn't exist returns a null document.
     */
    @Category(DaoTestedMethod.FindOne.class)
    @Test
    public void findOneMissingDocument() {

        ID id = getIdFieldValue(getTestFixture().newDocumentToInsert());
        T found = getRepository().findOne(id);

        assertNull(found);
    }

    /**
     * Finding a document that exists returns this document.
     */
    @Category(DaoTestedMethod.FindOne.class)
    @Test
    public void findOneExistingDocument() {

        T document = getCustomizer().customizeFindOneFixture(getTestFixture().newExistingDocument());
        T found = getRepository().findOne(getIdFieldValue(document));

        assertNotNull(found);
        assertEquals(document, found);
    }


    /**
     * Testing the existence of a document with a null identifier throws an ActionRequestValidationException.
     */
    @Category(DaoTestedMethod.Exist.class)
    @Test(expected = ActionRequestValidationException.class)
    public void existOneNullDocument() {

        getRepository().exists(null);

        fail("Should thrown an ActionRequestValidationException");
    }

    /**
     * Testing the existence of a document that doesn't exist returns false.
     */
    @Category(DaoTestedMethod.Exist.class)
    @Test
    public void existOneMissingDocument() {

        ID id = getIdFieldValue(getTestFixture().newDocumentToInsert());
        boolean exists = getRepository().exists(id);

        assertFalse(exists);
    }

    /**
     * Testing the existence of a document that exists returns true.
     */
    @Category(DaoTestedMethod.Exist.class)
    @Test
    public void existOneExistingDocument() {

        ID id = getIdFieldValue(getTestFixture().newDocumentToUpdate());
        boolean exists = getRepository().exists(id);

        assertTrue(exists);
    }

    /**
     * Counting the number of documents returns the number of documents in the index type
     */
    @Category(DaoTestedMethod.Count.class)
    @Test
    public void countDocuments() {
        assertEquals(testDocumentsLoader.getLoadedDocumentCount(), getRepository().count());
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                     Tests related to document deletion                                         */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Deleting all documents leaves an empty index type.
     */
    @Category(DaoTestedMethod.DeleteAll.class)
    @Test
    public void deleteAllDocuments() {

        getRepository().deleteAll();

        assertEquals(0, testClientOperations.countDocuments(getDocumentMetadata()));

        // Customizable test function
        getCustomizer().customizeDeleteAll();
    }

    /**
     * Deleting a missing document set doesn't remove these documents from the index type.
     */
    @Test
    @Category(DaoTestedMethod.DeleteAllById.class)
    public void deletingMissingDocumentSet() {

        getRepository().delete(Collections.singletonList(getTestFixture().newDocumentToInsert()));

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount(),
                testClientOperations.countDocuments(getDocumentMetadata()));
    }

    /**
     * Deleting an existing document set removes these documents from the index type.
     */
    @Category(DaoTestedMethod.DeleteAllById.class)
    @Test
    public void deleteExistingDocumentSet() {

        List<T> documentsToDelete = Collections.singletonList(getTestFixture().newExistingDocument());
        getRepository().delete(documentsToDelete);

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount() - 1,
                testClientOperations.countDocuments(getDocumentMetadata()));

        // Customizable test function
        getCustomizer().customizeDeleteSet(documentsToDelete);
    }

    /**
     * Deleting a missing document set doesn't remove this document from the index type.
     */
    @Category(DaoTestedMethod.Delete.class)
    @Test
    public void deleteOneMissingDocument() {

        getRepository().delete(getTestFixture().newDocumentToInsert());

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount(),
                testClientOperations.countDocuments(getDocumentMetadata()));
    }

    /**
     * Deleting an existing document set removes this document from the index type.
     */
    @Category(DaoTestedMethod.Delete.class)
    @Test
    public void deleteOneExistingDocument() {

        T documentToUpdate = getTestFixture().newDocumentToUpdate();
        ID id = getIdFieldValue(documentToUpdate);
        getRepository().delete(documentToUpdate);

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount() - 1,
                testClientOperations.countDocuments(getDocumentMetadata()));

        // Customizable test function
        getCustomizer().customizeDeleteOne(String.class.cast(id));
    }

    /**
     * Deleting a missing document by identifier set doesn't remove this document from the index type.
     */
    @Category(DaoTestedMethod.DeleteById.class)
    @Test
    public void deleteOneMissingDocumentById() {

        getRepository().delete(getIdFieldValue(getTestFixture().newDocumentToInsert()));

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount(),
                testClientOperations.countDocuments(getDocumentMetadata()));
    }

    /**
     * Deleting an existing document by identifier set removes this document from the index type.
     */
    @Category(DaoTestedMethod.DeleteById.class)
    @Test
    public void deleteOneExistingDocumentById() {

        ID id = getIdFieldValue(getTestFixture().newDocumentToUpdate());
        getRepository().delete(id);

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount() - 1,
                testClientOperations.countDocuments(getDocumentMetadata()));

        // Customizable test function
        getCustomizer().customizeDeleteOne(String.class.cast(id));
    }
}
