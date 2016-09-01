package info.jallaix.spring.data.es.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Validation error used for testing
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ValidationError {

    private String entity;
    private String message;
    private String invalidValue;
    private String property;
}
