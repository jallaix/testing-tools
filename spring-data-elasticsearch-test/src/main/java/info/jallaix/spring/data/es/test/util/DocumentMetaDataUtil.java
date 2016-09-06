package info.jallaix.spring.data.es.test.util;

import lombok.Getter;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import java.lang.reflect.Field;

/**
 * Utility class about Elasticsearch document metadata.
 */
public class DocumentMetaDataUtil<T> {

    @Getter
    private final Class<T> documentClass;

    /**
     * Constructor that stores the document class
     */
    public DocumentMetaDataUtil(Class<T> documentClass) {
        this.documentClass = documentClass;
    }

    /**
     * Find Elastic metadata (index, type, ...) of the tested document
     * @return The document metadata
     */
    public Document getDocumentMetadata() {
        return documentClass.getDeclaredAnnotation(Document.class); // Get annotation from document class
    }

    /**
     * Find the field used as identifier for the document
     * @return The identifier field
     */
    public Field getIdFieldForDocument() {

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
