package info.jallaix.spring.data.es.test;

import org.elasticsearch.action.ActionRequestValidationException;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
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
 * Test class for the Spring Data Elasticsearch module.<br/>
 * It supports data initialization thanks to the <a href="https://github.com/tlrx/elasticsearch-test">elasticsearch-test framework</a>.<br/>
 * It also performs generic CRUD tests on the tested repository.<br/><br/>
 *
 * The repository must verify the following tests related to document <b>indexing</b> or <b>saving</b> (same behavior) :
 * <ul>
 *     <li>Indexing a null document throws an IllegalArgumentException.</li>
 *     <li>Saving a null document throws an IllegalArgumentException.</li>
 *     <li>Saving a list of documents with one null throws an IllegalArgumentException and no document is indexed.</li>
 *     <li>Indexing a new document inserts the document in the index.</li>
 *     <li>Saving a new document inserts the document in the index.</li>
 *     <li>Saving a list of new documents inserts the documents in the index.</li>
 *     <li>Indexing an existing document replaces the document in the index.</li>
 *     <li>Saving an existing document replaces the document in the index.</li>
 *     <li>Saving a list of existing documents replaces the documents in the index.</li>
 * </ul>
 * The repository must verify the following tests related to document <b>finding</b> :
 * <ul>
 *     <li>Finding a list of all existing documents returns an iterable with all these documents.</li>
 *     <li>Finding a list of existing documents by identifier returns an iterable with all these documents.</li>
 *     <li>Finding a page of sorted existing documents returns an iterable with all these sorted documents.</li>
 *     <li>Finding a page of existing documents returns an iterable with all these documents.</li>
 *     <li>Finding a document with a null identifier throws an ActionRequestValidationException.</li>
 *     <li>Finding a document that doesn't exist returns a null document.</li>
 *     <li>Finding a document that exists returns this document.</li>
 *     <li>Testing the existence of a document with a null identifier throws an ActionRequestValidationException.</li>
 *     <li>Testing the existence of a document that doesn't exist returns false.</li>
 *     <li>Testing the existence of a document that exists returns true.</li>
 *     <li>Counting the number of documents returns the number of documents in the index type</li>
 * </ul>
 * The repository must verify the following tests related to document <b>deleting</b> :
 * <ul>
 *     <li>Deleting all documents leaves an empty index type.</li>
 *     <li>Deleting a missing document set doesn't remove these documents from the index type.</li>
 *     <li>Deleting an existing document set removes these documents from the index type.</li>
 *     <li>Deleting a missing document set doesn't remove this document from the index type.</li>
 *     <li>Deleting an existing document set removes this document from the index type.</li>
 *     <li>Deleting a missing document by identifier set doesn't remove this document from the index type.</li>
 *     <li>Deleting an existing document by identifier set removes this document from the index type.</li>
 * </ul>
 */
public abstract class SpringDataEsCrudTestCase<T, ID extends Serializable, R extends ElasticsearchRepository<T, ID>> extends SpringDataEsTestCase<T, ID, R> {

    @Rule
    public TestName name = new TestName();

    /**
     * Test documents loader
     */
    @Autowired
    private TestDocumentsLoader testDocumentsLoader;

    /**
     * Test client operations
     */
    @Autowired
    private TestClientOperations testClientOperations;


    @Before
    public void selectTests() {

        Assume.assumeTrue(isTestPlayed());
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                           Ignored tests system                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Set of tested methods
     */
    private Set<Class<? extends TestedMethod>> testedMethods;

    /**
     * Constructor with list of methods to test
     * @param methods Methods to test
     */
    @SafeVarargs
    public SpringDataEsCrudTestCase(Class<? extends TestedMethod>... methods) {

        testedMethods = new HashSet<>(Arrays.asList(methods));
    }

    /**
     * Determine if a test is played
     * @return {@code true} if the test has to be played else {@code false}
     */
    private boolean isTestPlayed() {

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
    /*                                    Tests related to document indexing                                          */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Indexing a null document throws an IllegalArgumentException.
     */
    @Category(TestedMethod.Index.class)
    @Test(expected=IllegalArgumentException.class)
    public void indexNullDocument() {

        repository.index(null);
    }

    /**
     * Saving a null document throws an IllegalArgumentException.
     */
    @Category(TestedMethod.Save.class)
    @Test(expected=IllegalArgumentException.class)
    public void saveNullDocument() {

        repository.save((T)null);
    }

    /**
     * Indexing a list of documents with one null throws an IllegalArgumentException and no document is indexed.
     */
    @Category(TestedMethod.SaveBulk.class)
    @Test
    public void saveNullDocuments() {

        List<T> documents = new ArrayList<>(1);
        documents.add(newDocumentToInsert());
        documents.add(null);

        try {
            repository.save(documents);
            fail("IllegalArgumentException must be thrown");
        }
        catch (IllegalArgumentException e) {
            assertEquals(
                    testDocumentsLoader.getLoadedDocumentCount(),
                    testClientOperations.countDocuments(documentMetadata));
        }
    }

    /**
     * Indexing a new document inserts the document in the index.
     */
    @Category(TestedMethod.Index.class)
    @Test
    public void indexNewDocument() {

        T toInsert = newDocumentToInsert();
        T inserted = repository.index(toInsert);

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount() + 1,
                testClientOperations.countDocuments(documentMetadata));
        assertEquals(toInsert, inserted);
    }

    /**
     * Saving a new document inserts the document in the index.
     */
    @Category(TestedMethod.Save.class)
    @Test
    public void saveNewDocument() {

        T toInsert = newDocumentToInsert();
        T inserted = repository.save(toInsert);

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount() + 1,
                testClientOperations.countDocuments(documentMetadata));
        assertEquals(toInsert, inserted);
    }

    /**
     * Saving a list of new documents inserts the documents in the index.
     */
    @Category(TestedMethod.Save.class)
    @Test
    public void saveNewDocuments() {

        List<T> toInsert = new ArrayList<>(1);
        toInsert.add(newDocumentToInsert());
        List<T> inserted = new ArrayList<>(1);
        repository.save(toInsert)
                .forEach(inserted::add);

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount() + 1,
                testClientOperations.countDocuments(documentMetadata));
        assertArrayEquals(toInsert.toArray(), inserted.toArray());
    }

    /**
     * Indexing an existing document replaces the document in the index.
     */
    @Category(TestedMethod.Index.class)
    @Test
    public void indexExistingDocument() {

        T toUpdate = newDocumentToUpdate();
        T updated = repository.index(toUpdate);

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount(),
                testClientOperations.countDocuments(documentMetadata));
        assertEquals(toUpdate, updated);
    }

    /**
     * Saving an existing document replaces the document in the index.
     */
    @Category(TestedMethod.Save.class)
    @Test
    public void saveExistingDocument() {

        T toUpdate = newDocumentToUpdate();
        T updated = repository.save(toUpdate);

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount(),
                testClientOperations.countDocuments(documentMetadata));
        assertEquals(toUpdate, updated);
    }

    /**
     * Saving a list of existing documents replaces the documents in the index.
     */
    @Category(TestedMethod.SaveBulk.class)
    @Test
    public void saveExistingDocuments() {

        List<T> toUpdate = new ArrayList<>(1);
        toUpdate.add(newDocumentToUpdate());
        List<T> updated = new ArrayList<>(1);
        repository.save(toUpdate)
                .forEach(updated::add);

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount(),
                testClientOperations.countDocuments(documentMetadata));
        assertArrayEquals(toUpdate.toArray(), updated.toArray());
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                     Tests related to document finding                                          */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Finding a list of all existing documents returns an iterable with all these documents.
     */
    @Category(TestedMethod.FindAll.class)
    @Test
    public void findAllDocuments() {

        List<T> initialList = testClientOperations.findAllDocuments(documentMetadata, documentClass);

        List<T> foundList = new ArrayList<>();
        repository.findAll()
                .forEach(foundList::add);

        assertArrayEquals(initialList.toArray(), foundList.toArray());
    }

    /**
     * Finding a list of existing documents by identifier returns an iterable with all these documents.
     */
    @Category(TestedMethod.FindAllById.class)
    @Test
    public void findAllDocumentsByIdentifier() {

        List<T> initialList = testClientOperations.findAllDocuments(documentMetadata, documentClass);
        List<ID> initialKeys = initialList.stream()
                .map(this::getIdFieldValue)
                .collect(Collectors.toList());

        List<T> foundList = new ArrayList<>();
        repository.findAll(initialKeys)
                .forEach(foundList::add);

        assertArrayEquals(initialList.toArray(), foundList.toArray());
    }

    /**
     * Finding a page of sorted existing documents returns an iterable with all these sorted documents.
     */
    @Category(TestedMethod.FindAllPageable.class)
    @Test
    public void findAllDocumentsByPage() {

        // Define the page parameters
        long documentsCount = testClientOperations.countDocuments(documentMetadata);
        Assert.isTrue(documentsCount > 0, "No document loaded");
        int pageSize = getPageSize();
        Assert.isTrue(pageSize > 0, "Page size must be positive");
        int nbPages = (int) documentsCount / pageSize + (documentsCount % pageSize == 0 ? 0 : 1);

        // Define sorting parameter
        Sort sorting = new Sort(Sort.Direction.DESC, documentIdField.getName());

        // Find first page
        List<T> initialList = testClientOperations.findAllDocumentsByPage(
                documentMetadata,
                documentClass,
                documentIdField,
                0,
                pageSize);
        List<T> foundList = new ArrayList<>();
        repository.findAll(new PageRequest(0, pageSize, sorting))
                .forEach(foundList::add);

        assertArrayEquals(initialList.toArray(), foundList.toArray());

        // Find last page
        initialList = testClientOperations.findAllDocumentsByPage(
                documentMetadata,
                documentClass,
                documentIdField,
                nbPages - 1,
                pageSize);
        foundList.clear();
        repository.findAll(new PageRequest(nbPages - 1, pageSize, sorting))
                .forEach(foundList::add);

        assertArrayEquals(initialList.toArray(), foundList.toArray());
    }

    /**
     * Finding a page of existing documents returns an iterable with all these documents.
     */
    @Category(TestedMethod.FindAllSorted.class)
    @Test
    public void findAllDocumentsSorted() {

        // Define sorting parameter
        Sort sorting = new Sort(Sort.Direction.DESC, documentIdField.getName());

        // Find first page
        List<T> initialList = testClientOperations.findAllDocumentsSorted(
                documentMetadata,
                documentClass,
                documentIdField);
        List<T> foundList = new ArrayList<>();
        repository.findAll(sorting)
                .forEach(foundList::add);

        assertArrayEquals(initialList.toArray(), foundList.toArray());
    }

    /**
     * Finding a document with a null identifier throws an ActionRequestValidationException.
     */
    @Category(TestedMethod.FindOne.class)
    @Test(expected=ActionRequestValidationException.class)
    public void findOneNullDocument() {

        repository.findOne(null);

        fail("Should thrown an ActionRequestValidationException");
    }

    /**
     * Finding a document that doesn't exist returns a null document.
     */
    @Category(TestedMethod.FindOne.class)
    @Test
    public void findOneMissingDocument() {

        ID id = getIdFieldValue(newDocumentToInsert());
        T found = repository.findOne(id);

        assertNull(found);
    }

    /**
     * Finding a document that exists returns this document.
     */
    @Category(TestedMethod.FindOne.class)
    @Test
    public void findOneExistingDocument() {

        T document = newDocumentToUpdate();
        ID id = getIdFieldValue(document);

        T found = repository.findOne(id);
        ID foundId = getIdFieldValue(document);

        assertNotNull(found);
        assertEquals(id, foundId);
    }

    /**
     * Testing the existence of a document with a null identifier throws an ActionRequestValidationException.
     */
    @Category(TestedMethod.Exist.class)
    @Test(expected=ActionRequestValidationException.class)
    public void existOneNullDocument() {

        repository.exists(null);

        fail("Should thrown an ActionRequestValidationException");
    }

    /**
     * Testing the existence of a document that doesn't exist returns false.
     */
    @Category(TestedMethod.Exist.class)
    @Test
    public void existOneMissingDocument() {

        ID id = getIdFieldValue(newDocumentToInsert());
        boolean exists = repository.exists(id);

        assertFalse(exists);
    }

    /**
     * Testing the existence of a document that exists returns true.
     */
    @Category(TestedMethod.Exist.class)
    @Test
    public void existOneExistingDocument() {

        ID id = getIdFieldValue(newDocumentToUpdate());
        boolean exists = repository.exists(id);

        assertTrue(exists);
    }

    /**
     * Counting the number of documents returns the number of documents in the index type
     */
    @Category(TestedMethod.Count.class)
    @Test
    public void countDocuments() {

        assertEquals(testDocumentsLoader.getLoadedDocumentCount(), repository.count());
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                     Tests related to document deleting                                         */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Deleting all documents leaves an empty index type.
     */
    @Category(TestedMethod.DeleteAll.class)
    @Test
    public void deleteAllDocuments() {

        repository.deleteAll();

        assertEquals(0, testClientOperations.countDocuments(documentMetadata));
    }

    /**
     * Deleting a missing document set doesn't remove these documents from the index type.
     */
    @Test
    @Category(TestedMethod.DeleteAllById.class)
    public void deletingMissingDocumentSet() {

        repository.delete(Collections.singletonList(newDocumentToInsert()));

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount(),
                testClientOperations.countDocuments(documentMetadata));
    }

    /**
     * Deleting an existing document set removes these documents from the index type.
     */
    @Category(TestedMethod.DeleteAllById.class)
    @Test
    public void deleteExistingDocumentSet() {

        repository.delete(Collections.singletonList(newDocumentToUpdate()));

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount() - 1,
                testClientOperations.countDocuments(documentMetadata));
    }

    /**
     * Deleting a missing document set doesn't remove this document from the index type.
     */
    @Category(TestedMethod.Delete.class)
    @Test
    public void deleteOneMissingDocument() {

        repository.delete(newDocumentToInsert());

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount(),
                testClientOperations.countDocuments(documentMetadata));
    }

    /**
     * Deleting an existing document set removes this document from the index type.
     */
    @Category(TestedMethod.Delete.class)
    @Test
    public void deleteOneExistingDocument() {

        repository.delete(newDocumentToUpdate());

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount() - 1,
                testClientOperations.countDocuments(documentMetadata));
    }

    /**
     * Deleting a missing document by identifier set doesn't remove this document from the index type.
     */
    @Category(TestedMethod.DeleteById.class)
    @Test
    public void deleteOneMissingDocumentById() {

        repository.delete(getIdFieldValue(newDocumentToInsert()));

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount(),
                testClientOperations.countDocuments(documentMetadata));
    }

    /**
     * Deleting an existing document by identifier set removes this document from the index type.
     */
    @Category(TestedMethod.DeleteById.class)
    @Test
    public void deleteOneExistingDocumentById() {

        repository.delete(getIdFieldValue(newDocumentToUpdate()));

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount() - 1,
                testClientOperations.countDocuments(documentMetadata));
    }
}
