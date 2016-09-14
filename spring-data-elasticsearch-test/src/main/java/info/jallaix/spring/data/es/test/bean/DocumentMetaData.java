package info.jallaix.spring.data.es.test.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.elasticsearch.annotations.Document;

import java.lang.reflect.Field;

/**
 * Document metadata.
 * <ul>
 *    <li>{@code documentClass} - {@link Class<T>} - Class of the document
 *    <li>{@code documentAnnotation} - {@link Document} - Annotation properties of the document
 *    <li>{@code documentIdField} - {@link Field} - Identifier field of the document
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DocumentMetaData<T> {

    private Class<T> documentClass;
    private Document documentAnnotation;
    private Field documentIdField;
}
