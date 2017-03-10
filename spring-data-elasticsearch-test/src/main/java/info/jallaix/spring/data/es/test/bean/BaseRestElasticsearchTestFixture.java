package info.jallaix.spring.data.es.test.bean;

import org.springframework.hateoas.Resource;
import org.springframework.hateoas.mvc.TypeReferences;

import java.util.List;
import java.util.Map;

/**
 * Created by JAX on 10/03/2017.
 */
public interface BaseRestElasticsearchTestFixture<T> {

    /**
     * Return an object with some getters matching the {@link T} entity getters.
     * The values returned by the getters must be different than those returned by the
     * {@link BaseElasticsearchTestFixture#newExistingDocument()} getters so that patching tests may occur.
     *
     * @return An object with some getters matching the {@link T} entity getters.
     */
    Object newObjectForPatch();

    /**
     * Get the resource type for JSON/Object mapping.
     *
     * @return The resource type
     */
    TypeReferences.ResourceType<T> getResourceType();

    /**
     * Get the paged resources type for JSON/Object mapping.
     *
     * @return The paged resources type
     */
    TypeReferences.PagedResourcesType<Resource<T>> getPagedResourcesType();

    /**
     * Return a map of entities linked to a list of expected validation errors that occur when attempting to create an entity.
     * Each entity must hold a set of properties that causes some validation errors to occur.
     *
     * @return The map of entities linked to a list of expected validation errors
     */
    Map<T, List<ValidationError>> getExpectedValidationErrorsOnCreate();

    /**
     * Return a map of entities linked to a list of expected validation errors that occur when attempting to update an entity.
     * Each entity must hold a set of properties that causes some validation errors to occur.
     *
     * @return The map of entities linked to a list of expected validation errors
     */
    Map<T, List<ValidationError>> getExpectedValidationErrorsOnUpdate();

    /**
     * Return a map of objects linked to a list of expected validation errors that occur when attempting to patch an entity.
     * Each object must hold a set of properties that causes some validation errors to occur.
     *
     * @return The map of objects linked to a list of expected validation errors
     */
    Map<Object, List<ValidationError>> getExpectedValidationErrorsOnPatch();

    /**
     * Return a map of entities linked to a list of expected validation errors that occur when attempting to delete an entity.
     * Each entity must hold a set of properties that causes some validation errors to occur.
     *
     * @return The map of entities linked to a list of expected validation errors
     */
    Map<T, List<ValidationError>> getExpectedValidationErrorsOnDelete();
}
