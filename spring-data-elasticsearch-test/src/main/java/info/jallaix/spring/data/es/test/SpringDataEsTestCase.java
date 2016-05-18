package info.jallaix.spring.data.es.test;

import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.test.context.ContextConfiguration;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test class for the Spring Data Elasticsearch module.<br/>
 * It supports data initialization thanks to the <a href="https://github.com/tlrx/elasticsearch-test">elasticsearch-test framework</a>.<br/>
 * It also performs generic CRUD tests on the tested repository.<br/><br/>
 * The repository must verify the following tests related to <b>indexing</b> or <b>saving</b> that have the same behavior :
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
 */
@ContextConfiguration(classes = SpringDataEsTestConfiguration.class)
public abstract class SpringDataEsTestCase<T, ID extends Serializable, R extends ElasticsearchRepository<T, ID>> {

    /**
     * Test documents loader
     */
    @Autowired
    private TestDocumentsLoader testDocumentsLoader;

    /**
     * Elasticsearch client
     */
    @Autowired
    private Client esClient;


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                           Pre-test data loading                                                */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Create an Elasticsearch index and type and load custom data in it.
     */
    @Before
    public void initElasticIndex() {

        documentMetadata = findDocumentMetadata();          // Find document metadata (index, type)
        testDocumentsLoader.initElasticIndex(               // Load documents into index
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
     * Get the Elastic document metadata
     * @return The Elastic document metadata
     */
    @SuppressWarnings("unused")
    protected Document getDocumentMetadata() { return documentMetadata; }

    /**
     * Find Elastic metadata (index, type, ...) of the tested document
     * @return The document metadata
     */
    private Document findDocumentMetadata() {

        // Find document class
        ParameterizedType superClass = (ParameterizedType) getClass().getGenericSuperclass();
        Type [] types = superClass.getActualTypeArguments();
        @SuppressWarnings("unchecked")
        Class<T> documentClass = (Class<T>) types[0];

        // Get annotation from document class
        return documentClass.getDeclaredAnnotation(Document.class);
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
    /*                                             Sub-classes methods                                                */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Count the number of typed documents in the index.
     * @return The number of typed documents found
     */
    protected long countDocuments() {

        return esClient.prepareCount(documentMetadata.indexName()).setTypes(documentMetadata.type()).get().getCount();
    }

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


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                    Tests related to language indexing                                          */
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
            assertEquals(testDocumentsLoader.getLoadedDocumentCount(), countDocuments());
        }
    }

    /**
     * Indexing a new document inserts the document in the index.
     */
    @Test
    public void indexNewDocument() {

        T toInsert = newDocumentToInsert();
        T inserted = repository.index(toInsert);

        assertEquals(testDocumentsLoader.getLoadedDocumentCount() + 1, countDocuments());
        assertEquals(toInsert, inserted);
    }

    /**
     * Saving a new document inserts the document in the index.
     */
    @Test
    public void saveNewDocument() {

        T toInsert = newDocumentToInsert();
        T inserted = repository.save(toInsert);

        assertEquals(testDocumentsLoader.getLoadedDocumentCount() + 1, countDocuments());
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
        repository.save(toInsert).forEach(inserted::add);

        assertEquals(testDocumentsLoader.getLoadedDocumentCount() + 1, countDocuments());
        assertArrayEquals(toInsert.toArray(), inserted.toArray());
    }

    /**
     * Indexing an existing document replaces the document in the index.
     */
    @Test
    public void indexExistingDocument() {

        T toUpdate = newDocumentToUpdate();
        T updated = repository.index(toUpdate);

        assertEquals(testDocumentsLoader.getLoadedDocumentCount(), countDocuments());
        assertEquals(toUpdate, updated);
    }

    /**
     * Saving an existing document replaces the document in the index.
     */
    @Test
    public void saveExistingDocument() {

        T toUpdate = newDocumentToUpdate();
        T updated = repository.save(toUpdate);

        assertEquals(testDocumentsLoader.getLoadedDocumentCount(), countDocuments());
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
        repository.save(toUpdate).forEach(updated::add);

        assertEquals(testDocumentsLoader.getLoadedDocumentCount(), countDocuments());
        assertArrayEquals(toUpdate.toArray(), updated.toArray());
    }
}
