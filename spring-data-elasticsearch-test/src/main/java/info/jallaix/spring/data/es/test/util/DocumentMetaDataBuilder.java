package info.jallaix.spring.data.es.test.util;

import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentEntity;
import org.springframework.util.Assert;

/**
 * Utility class about Elasticsearch document metadata.
 */
public class DocumentMetaDataBuilder {

    /**
     * <p>
     * Build a bean holding document metadata.
     * </p>
     * <p>
     * Uses the same code as the {@link org.springframework.data.elasticsearch.core.ElasticsearchTemplate#getPersistentEntityFor(Class)} private method.
     * </p>
     *
     * @param esOperations  Elasticsearch operations
     * @param documentClass The document class
     * @return The document metadata
     */
    public static ElasticsearchPersistentEntity buildDocumentMetadata(ElasticsearchOperations esOperations, Class<?> documentClass) {

        Assert.isTrue(documentClass.isAnnotationPresent(Document.class), "Unable to identify index name. " + documentClass.getSimpleName()
                + " is not a Document. Make sure the document class is annotated with @Document(indexName=\"foo\")");

        return esOperations.getElasticsearchConverter().getMappingContext().getPersistentEntity(documentClass);
    }
}
