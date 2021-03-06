package info.jallaix.spring.data.es.test.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Validation error used for testing.
 * <ul>
 *    <li>{@code entity} - {@link String} - Simple entity name
 *    <li>{@code message} - {@link String} - Error message
 *    <li>{@code invalidValue} - {@link String} - Entity value causing the validation error
 *    <li>{@code property} - {@link String} - Entity property causing the validation error
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ValidationError {

    /**
     * Simple entity name
     */
    private String entity;

    /**
     * Error message
     */
    private String message;

    /**
     * Entity value causing the validation error
     */
    private String invalidValue;

    /**
     * Entity property causing the validation error
     */
    private String property;
}
