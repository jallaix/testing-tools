package info.jallaix.spring.data.es.test.util;

import info.jallaix.spring.data.es.test.bean.DocumentMetaData;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import java.lang.reflect.Field;

/**
 * Utility class about Elasticsearch document metadata.
 */
public class DocumentMetaDataBuilder {

    /**
     * Build a bean holding document metadata.
     *
     * @param <T>           The document type
     * @param documentClass The document class
     * @return The document metadata
     */
    public static <T> DocumentMetaData<T> buildDocumentMetadata(Class<T> documentClass) {

        Document documentAnnotation = getDocumentAnnotation(documentClass);
        Field documentIdField = getIdentifierField(documentClass);

        return new DocumentMetaData<>(documentClass, documentAnnotation, documentIdField);
    }

    /**
     * Find Elastic metadata (index, type, ...) of the tested document.
     *
     * @param <T>           The document type
     * @param documentClass The document class
     * @return The document metadata
     */
    private static <T> Document getDocumentAnnotation(Class<T> documentClass) {
        return documentClass.getDeclaredAnnotation(Document.class); // Get annotation from document class
    }

    /**
     * Find the field used as identifier for the document.
     *
     * @param <T>           The document type
     * @param documentClass The document class
     * @return The identifier field
     */
    private static <T> Field getIdentifierField(Class<T> documentClass) {

        // Find field in document class with @Id annotation
        for (Field field : documentClass.getDeclaredFields()) {
            if (field.getDeclaredAnnotation(Id.class) != null) {
                field.setAccessible(true);
                return field;
            }
        }

        return null;
    }
}
