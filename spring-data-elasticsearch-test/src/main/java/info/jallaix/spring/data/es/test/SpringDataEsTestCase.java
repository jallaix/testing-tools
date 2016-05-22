package info.jallaix.spring.data.es.test;

import org.elasticsearch.action.ActionRequestValidationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.util.Assert;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
public abstract class SpringDataEsTestCase<T, ID extends Serializable, R extends ElasticsearchRepository<T, ID>> {


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


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                           Tested document metadata                                             */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Elastic document metadata
     */
    private Document documentMetadata;

    /**
     * Class of the document
     */
    private Class<T> documentClass;

    /**
     * Document identifier
     */
    private Field documentIdField;

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
        Type [] types = superClass.getActualTypeArguments();
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
    private R repository;

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


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                    Tests related to document indexing                                          */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Indexing a null document throws an IllegalArgumentException.
     */
    @Test(expected=IllegalArgumentException.class)
    public void indexNullDocument() {

        repository.index(null);
    }

    /**
     * Saving a null document throws an IllegalArgumentException.
     */
    @Test(expected=IllegalArgumentException.class)
    public void saveNullDocument() {

        repository.save((T)null);
    }

    /**
     * Indexing a list of documents with one null throws an IllegalArgumentException and no document is indexed.
     */
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
    @Test(expected=ActionRequestValidationException.class)
    public void findOneNullDocument() {

        repository.findOne(null);

        fail("Should thrown an ActionRequestValidationException");
    }

    /**
     * Finding a document that doesn't exist returns a null document.
     */
    @Test
    public void findOneMissingDocument() {

        ID id = getIdFieldValue(newDocumentToInsert());
        T found = repository.findOne(id);

        assertNull(found);
    }

    /**
     * Finding a document that exists returns this document.
     */
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
    @Test(expected=ActionRequestValidationException.class)
    public void existOneNullDocument() {

        repository.exists(null);

        fail("Should thrown an ActionRequestValidationException");
    }

    /**
     * Testing the existence of a document that doesn't exist returns false.
     */
    @Test
    public void existOneMissingDocument() {

        ID id = getIdFieldValue(newDocumentToInsert());
        boolean exists = repository.exists(id);

        assertFalse(exists);
    }

    /**
     * Testing the existence of a document that exists returns true.
     */
    @Test
    public void existOneExistingDocument() {

        ID id = getIdFieldValue(newDocumentToUpdate());
        boolean exists = repository.exists(id);

        assertTrue(exists);
    }

    /**
     * Counting the number of documents returns the number of documents in the index type
     */
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
    @Test
    public void deleteAllDocuments() {

        repository.deleteAll();

        assertEquals(0, testClientOperations.countDocuments(documentMetadata));
    }

    /**
     * Deleting a missing document set doesn't remove these documents from the index type.
     */
    @Test
    public void deletingMissingDocumentSet() {

        repository.delete(Collections.singletonList(newDocumentToInsert()));

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount(),
                testClientOperations.countDocuments(documentMetadata));
    }

    /**
     * Deleting an existing document set removes these documents from the index type.
     */
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
    @Test
    public void deleteOneExistingDocumentById() {

        repository.delete(getIdFieldValue(newDocumentToUpdate()));

        assertEquals(
                testDocumentsLoader.getLoadedDocumentCount() - 1,
                testClientOperations.countDocuments(documentMetadata));
    }
}
