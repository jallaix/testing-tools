package info.jallaix.spring.data.es.test.testcase;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatchException;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.github.fge.jsonpatch.mergepatch.JsonMergePatch;
import info.jallaix.spring.data.es.test.bean.ValidationError;
import info.jallaix.spring.data.es.test.util.TestClientOperations;
import org.apache.commons.codec.Charsets;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.data.rest.webmvc.RestMediaTypes;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.PagedResources;
import org.springframework.hateoas.Resource;
import org.springframework.hateoas.mvc.TypeReferences;
import org.springframework.http.*;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * <p/>
 * Test class for the Spring Data REST Elasticsearch module.<br>
 * It supports data initialization thanks to the <a href="https://github.com/tlrx/elasticsearch-test">elasticsearch-test framework</a>.<br>
 * It also performs generic CRUD (POST, GET, PUT, DELETE) tests on the tested repository.<br>
 * <p/>
 * <p/>
 * The REST web service must verify the following tests related to <b>entity creation</b> (POST) :
 * <ul>
 * <li>Creating an entity returns a {@code 400 Bad Request} HTTP status code if no entity data is provided.</li>
 * <li>
 * Creating an entity returns a {@code 400 Bad Request} HTTP status code if it contains invalid fields.
 * Invalid entity properties are defined by the {@link BaseRestElasticsearchTestCase#getExpectedValidationErrorsOnCreateOrUpdate} method.
 * </li>
 * <li>
 * Creating an entity returns a {@code 409 Conflict} HTTP status code if it already exists.
 * The existing entity is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
 * </li>
 * <li>
 * Creating an entity returns a {@code 201 Created} HTTP status code if the entry doesn't already exist and the entity argument is valid.
 * The entity to insert is defined by the {@link BaseRestElasticsearchTestCase#newDocumentToInsert()} method.
 * </li>
 * </ul>
 * <p/>
 * <p/>
 * The REST web service must verify the following tests related to <b>entity search</b> :
 * <ul>
 * <li>
 * Getting an entity returns a {@code 404 Not Found} HTTP status code if there is no entity found.
 * The missing entity is defined by the {@link BaseRestElasticsearchTestCase#newDocumentToInsert()} method.
 * </li>
 * <li>
 * Getting an entity returns this entity in HATEOAS format and a {@code 200 Ok} HTTP status code if the entity is found.
 * The existing entity is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
 * </li>
 * <li>Getting all entities returns these entities in HATEOAS format and a {@code 200 Ok} HTTP status code.</li>
 * <li>
 * Getting all entities sorted returns these entities in HATEOAS format and a {@code 200 Ok} HTTP status code.
 * The sort field if defined by the {@link BaseRestElasticsearchTestCase#getSortField()} method.
 * </li>
 * <li>
 * Getting all entities paged returns these entities in HATEOAS format and a {@code 200 Ok} HTTP status code.
 * The page size if defined by the {@link BaseRestElasticsearchTestCase#getPageSize()} method.
 * </li>
 * <li>
 * Getting all entities sorted and paged returns these entities in HATEOAS format and a {@code 200 Ok} HTTP status code.
 * The sort field if defined by the {@link BaseRestElasticsearchTestCase#getSortField()} method.
 * The page size if defined by the {@link BaseRestElasticsearchTestCase#getPageSize()} method.
 * </li>
 * </ul>
 * <p/>
 * <p/>
 * The REST web service must verify the following tests related to <b>entity existence</b> :
 * <ul>
 * <li>HEADing an existing entity returns a {@code 204 No Content} HTTP status code.</li>
 * <li>HEADing a missing entity returns a {@code 404 Not Found} HTTP status code.</li>
 * <li>HEADing an entity collection returns a {@code 204 No Content} HTTP status code.</li>
 * </ul>
 * <p/>
 * <p/>
 * The REST web service must verify the following tests related to <b>entity update</b> :
 * <ul>
 * <li>
 * Updating an entity returns a {@code 405 Method Not Allowed} HTTP status code if no identifier is provided.
 * The existing entity is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
 * </li>
 * <li>
 * Updating an entity returns a {@code 400 Bad Request} HTTP status code if no entity is provided.
 * The existing entity identifier is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
 * </li>
 * <li>
 * Updating an entity returns {@code 404 Not Found} HTTP status code if there is no existing language to update.
 * The missing entity is defined by the {@link BaseRestElasticsearchTestCase#newDocumentToInsert()} method.
 * </li>
 * <li>
 * Updating an existing entity returns a {@code 200 Ok} HTTP status code as well as the updated resource that matches the resource in the request.
 * The entity to update is defined by the {@link BaseRestElasticsearchTestCase#newDocumentToUpdate()} method.
 * </li>
 * </ul>
 * <p/>
 * <p/>
 * The REST web service must verify the following tests related to <b>entity patching</b> :
 * <ul>
 * <li>
 * Patching an entity returns a {@code 405 Method Not Allowed} HTTP status code if no identifier is provided.
 * The existing entity is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
 * </li>
 * <li>
 * Patching an entity returns a {@code 400 Bad Request} HTTP status code if no entity is provided.
 * The existing entity identifier is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
 * </li>
 * <li>
 * Patching an entity returns {@code 404 Not Found} HTTP status code if there is no existing language to update.
 * The missing entity is defined by the {@link BaseRestElasticsearchTestCase#newDocumentToInsert()} method.
 * </li>
 * <li>
 * Patching an existing entity returns a {@code 200 Ok} HTTP status code as well as the updated resource that matches the resource in the request.
 * The entity to patch is defined by the {@link BaseRestElasticsearchTestCase#newDocumentToUpdate()} method.
 * </li>
 * </ul>
 * <p/>
 * <p/>
 * The REST web service must verify the following tests related to <b>entity deletion</b> :
 * <ul>
 * <li>Deleting an entity returns a {@code 405 Method Not Allowed } HTTP status code if no identifier is provided.</li>
 * <li>
 * Deleting an entity returns a {@code 404 Not Found } HTTP status code if it doesn't exist.
 * The entity to delete is defined by the {@link BaseRestElasticsearchTestCase#newDocumentToInsert()} method.
 * </li>
 * <li>
 * Deleting an entity returns a {@code 204 No Content } HTTP status code it exists and no validation error occurs.
 * The entity to delete is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
 * </li>
 */
@SuppressWarnings("unused")
public abstract class BaseRestElasticsearchTestCase<T, ID extends Serializable, R extends ElasticsearchRepository<T, ID>> extends BaseElasticsearchTestCase<T, ID, R> {

    /**
     * Media type for JSON Patch data
     */
    protected static final MediaType JSON_PATCH_JSON_UTF8 = new MediaType(RestMediaTypes.JSON_PATCH_JSON, Collections.singletonMap("charset", Charsets.UTF_8.displayName()));
    /**
     * Media type for JSON Merge Patch data
     */
    protected static final MediaType MERGE_PATCH_JSON_UTF8 = new MediaType(RestMediaTypes.MERGE_PATCH_JSON, Collections.singletonMap("charset", Charsets.UTF_8.displayName()));

    /**
     * Default page size for REST read operations
     */
    @Value("${spring.data.rest.default-page-size:20}")
    private int defaultPageSize;

    /**
     * Random server port
     */
    @Value("${local.server.port}")
    private int serverPort;

    /**
     * Test client operations
     */
    @SuppressWarnings("SpringJavaAutowiredMembersInspection")
    @Autowired
    private TestClientOperations testClientOperations;

    @SuppressWarnings("SpringJavaAutowiredMembersInspection")
    @Autowired
    private RestTemplate restTemplate;


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                           Ignored tests system                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Constructor with list of methods to test
     *
     * @param methods Methods to test
     */
    @SafeVarargs
    @SuppressWarnings("unused")
    public BaseRestElasticsearchTestCase(Class<? extends RestTestedMethod>... methods) {

        if (methods.length == 0)
            testedMethods = new HashSet<>(Arrays.asList(
                    RestTestedMethod.Create.class,
                    RestTestedMethod.Update.class,
                    RestTestedMethod.FindAll.class,
                    RestTestedMethod.FindAllPageable.class,
                    RestTestedMethod.FindOne.class,
                    RestTestedMethod.Exist.class,
                    RestTestedMethod.Delete.class));
        else
            testedMethods = new HashSet<>(Arrays.asList(methods));
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                             Abstract methods                                                   */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Get the resource type for JSON/Object mapping.
     *
     * @return The resource type
     */
    protected abstract TypeReferences.ResourceType<T> getResourceType();

    /**
     * Get the paged resources type for JSON/Object mapping.
     *
     * @return The paged resources type
     */
    protected abstract TypeReferences.PagedResourcesType<Resource<T>> getPagedResourcesType();

    /**
     * Return a map of entities linked to a list of expected validation errors.
     * Each entity must hold a set of properties that causes some validation errors to occur.
     *
     * @return The map of entities linked to a list of expected validation errors
     */
    protected abstract Map<T, List<ValidationError>> getExpectedValidationErrorsOnCreateOrUpdate();

    /**
     * Return a map of entities linked to a list of expected validation errors that occur when attempting to delete an entity.
     * Each entity must hold a set of properties that causes some validation errors to occur.
     *
     * @return The map of entities linked to a list of expected validation errors
     */
    protected abstract Map<T, List<ValidationError>> getExpectedValidationErrorsOnDelete();

    /**
     * Return a map of objects linked to a list of expected validation errors that occur when attempting to patch an entity.
     * Each object must hold a set of properties that causes some validation errors to occur.
     *
     * @return The map of objects linked to a list of expected validation errors
     */
    protected abstract Map<Object, List<ValidationError>> getExpectedValidationErrorsOnPatch();

    /**
     * Return an object with some getters matching the {@link T} entity getters.
     * The values returned by the getters must be different than those returned by the
     * {@link BaseElasticsearchTestCase#newExistingDocument()} getters so that patching tests may occur.
     *
     * @return An object with some getters matching the {@link T} entity getters.
     */
    protected abstract Object newObjectForPatch();


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                     Tests related to entity creation                                           */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Creating an entity returns a {@code 400 Bad Request} HTTP status code if no entity data is provided.
     */
    @Category(RestTestedMethod.Create.class)
    @Test
    public void createEmptyEntity() {
        postEntity(null, HttpStatus.BAD_REQUEST, true, null);
    }

    /**
     * Creating an entity returns a {@code 400 Bad Request} HTTP status code if it contains invalid fields.
     * Invalid entity properties are defined by the {@link BaseRestElasticsearchTestCase#getExpectedValidationErrorsOnCreateOrUpdate()} method.
     */
    @Category(RestTestedMethod.Create.class)
    @Test
    public void createInvalidEntity() {
        getExpectedValidationErrorsOnCreateOrUpdate().forEach((entity, errors) -> postEntity(entity, HttpStatus.BAD_REQUEST, true, errors));
    }

    /**
     * Creating an entity returns a {@code 409 Conflict} HTTP status code if it already exists.
     * The existing entity is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
     */
    @Category(RestTestedMethod.Create.class)
    @Test
    public void createDuplicateEntity() {
        postEntity(newExistingDocument(), HttpStatus.CONFLICT, true);
    }

    /**
     * Creating an entity returns a {@code 201 Created} HTTP status code if the entry doesn't already exist and the entity argument is valid.
     * The entity to insert is defined by the {@link BaseRestElasticsearchTestCase#newDocumentToInsert()} method.
     */
    @Category(RestTestedMethod.Create.class)
    @Test
    public void createValidEntity() {
        postEntity(newDocumentToInsert(), HttpStatus.CREATED, false);
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                      Tests related to entity search                                            */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Getting an entity returns a {@code 404 Not Found} HTTP status code if there is no entity found.
     * The missing entity is defined by the {@link BaseRestElasticsearchTestCase#newDocumentToInsert()} method.
     */
    @Category(RestTestedMethod.FindOne.class)
    @Test
    public void findMissingEntity() {
        getEntity(newDocumentToInsert(), HttpStatus.NOT_FOUND, true);
    }

    /**
     * Getting an entity returns this entity in HATEOAS format and a {@code 200 Ok} HTTP status code if the entity is found.
     * The existing entity is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
     */
    @Category(RestTestedMethod.FindOne.class)
    @Test
    public void findExistingEntity() {
        getEntity(newExistingDocument(), HttpStatus.OK, false);
    }

    /**
     * Getting all entities returns these entities in HATEOAS format and a {@code 200 Ok} HTTP status code.
     */
    @Category(RestTestedMethod.FindAll.class)
    @Test
    public void findEntities() {
        getEntities();
    }

    /**
     * Getting all entities sorted returns these entities in HATEOAS format and a {@code 200 Ok} HTTP status code.
     * The sort field if defined by the {@link BaseRestElasticsearchTestCase#getSortField()} method.
     */
    @Category(RestTestedMethod.FindAll.class)
    @Test
    public void findEntitiesSorted() {
        getEntities(true);
    }

    /**
     * Getting all entities paged returns these entities in HATEOAS format and a {@code 200 Ok} HTTP status code.
     * The page size if defined by the {@link BaseRestElasticsearchTestCase#getPageSize()} method.
     */
    @Category(RestTestedMethod.FindAllPageable.class)
    @Test
    public void findEntitiesPaged() {

        int totalPages = getTotalPages();
        for (int i = 0; i < totalPages; i++)
            getEntities(false, i);
    }

    /**
     * Getting all entities sorted and paged returns these entities in HATEOAS format and a {@code 200 Ok} HTTP status code.
     * The sort field if defined by the {@link BaseRestElasticsearchTestCase#getSortField()} method.
     * The page size if defined by the {@link BaseRestElasticsearchTestCase#getPageSize()} method.
     */
    @Category(RestTestedMethod.FindAllPageable.class)
    @Test
    public void findEntitiesPagedSorted() {

        int totalPages = getTotalPages();
        for (int i = 0; i < totalPages; i++)
            getEntities(true, i);
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                      Tests related to entity existence                                         */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * HEADing an existing entity returns a {@code 204 No Content} HTTP status code.
     */
    @Category(RestTestedMethod.Exist.class)
    @Test
    public void headExistingEntity() {
        headEntity(newExistingDocument(), HttpStatus.NO_CONTENT, false);
    }

    /**
     * HEADing a missing entity returns a {@code 404 Not Found} HTTP status code.
     */
    @Category(RestTestedMethod.Exist.class)
    @Test
    public void headMissingEntity() {
        headEntity(newDocumentToInsert(), HttpStatus.NOT_FOUND, true);
    }

    /**
     * HEADing an entity collection returns a {@code 204 No Content} HTTP status code.
     */
    @Category(RestTestedMethod.Exist.class)
    @Test
    public void headExistingEntities() {
        headEntities(HttpStatus.NO_CONTENT, false);
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                      Tests related to entity update                                            */
    /*----------------------------------------------------------------------------------------------------------------*/

    /*
     * Updating an entity returns a {@code 405 Method Not Allowed} HTTP status code if no identifier is provided.
     * The existing entity is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
     */
    @Category(RestTestedMethod.Update.class)
    @Test
    public void updateEntityWithoutId() {
        putEntity(null, HttpStatus.METHOD_NOT_ALLOWED, true);
    }

    /**
     * Updating an entity returns a {@code 400 Bad Request} HTTP status code if no entity is provided.
     * The existing entity identifier is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
     */
    @Category(RestTestedMethod.Update.class)
    @Test
    public void updateEmptyEntity() {
        putEntity(newExistingDocument(), HttpStatus.BAD_REQUEST, true, null, true);
    }

    /**
     * Updating an entity returns a {@code 400 Bad Request} HTTP status code if it contains invalid fields.
     * Invalid entity properties are defined by the {@link BaseRestElasticsearchTestCase#getExpectedValidationErrorsOnCreateOrUpdate()} method.
     */
    @Category(RestTestedMethod.Update.class)
    @Test
    public void updateInvalidEntity() {
        getExpectedValidationErrorsOnCreateOrUpdate().forEach((entity, errors) -> putEntity(entity, HttpStatus.BAD_REQUEST, true, errors));
    }

    /**
     * Updating an entity returns {@code 404 Not Found} HTTP status code if there is no existing entity to update.
     * The missing entity is defined by the {@link BaseRestElasticsearchTestCase#newDocumentToInsert()} method.
     */
    @Category(RestTestedMethod.Update.class)
    @Test
    public void updateMissingEntity() {
        putEntity(newDocumentToInsert(), HttpStatus.NOT_FOUND, true);
    }

    /**
     * Updating an existing entity returns a {@code 200 Ok} HTTP status code as well as the updated resource that matches the resource in the request.
     * The entity to update is defined by the {@link BaseRestElasticsearchTestCase#newDocumentToUpdate()} method.
     */
    @Category(RestTestedMethod.Update.class)
    @Test
    public void updateValidEntity() {
        putEntity(newDocumentToUpdate(), HttpStatus.OK, false);
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                     Tests related to entity patching                                           */
    /*----------------------------------------------------------------------------------------------------------------*/

    /*
     * Patching an entity returns a {@code 405 Method Not Allowed} HTTP status code if no identifier is provided.
     * The patch is defined by the {@link BaseRestElasticsearchTestCase#newObjectForPatch} method.
     */
    @Category(RestTestedMethod.Patch.class)
    @Test
    public void patchEntityWithoutId() {

        Object patch = newObjectForPatch();
        patchEntity(true, null, patch, HttpStatus.METHOD_NOT_ALLOWED, true);
        patchEntity(false, null, patch, HttpStatus.METHOD_NOT_ALLOWED, true);
    }

    /**
     * Patching an entity returns a {@code 400 Bad Request} HTTP status code if no entity is provided.
     * The existing entity identifier is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
     * The patch is defined by the {@link BaseRestElasticsearchTestCase#newObjectForPatch} method.
     */
    @Category(RestTestedMethod.Patch.class)
    @Test
    public void patchEmptyEntity() {

        Object patch = newObjectForPatch();
        patchEntity(true, newExistingDocument(), patch, HttpStatus.BAD_REQUEST, true, null, true);
        patchEntity(false, newExistingDocument(), patch, HttpStatus.BAD_REQUEST, true, null, true);
    }

    /**
     * Patching an entity returns a {@code 400 Bad Request} HTTP status code if it contains invalid fields.
     * The existing entity to patch is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
     * Invalid patch properties are defined by the {@link BaseRestElasticsearchTestCase#getExpectedValidationErrorsOnPatch()} method.
     */
    @Category(RestTestedMethod.Patch.class)
    @Test
    public void patchInvalidEntity() {

        T entity = newExistingDocument();
        getExpectedValidationErrorsOnPatch().forEach((patch, errors) -> patchEntity(true, entity, patch, HttpStatus.BAD_REQUEST, true, errors, false));
        getExpectedValidationErrorsOnPatch().forEach((patch, errors) -> patchEntity(false, entity, patch, HttpStatus.BAD_REQUEST, true, errors, false));
    }

    /**
     * Patching an entity returns {@code 404 Not Found} HTTP status code if there is no existing entity to patch.
     * The missing entity is defined by the {@link BaseRestElasticsearchTestCase#newDocumentToInsert()} method.
     * The patch is defined by the {@link BaseRestElasticsearchTestCase#newObjectForPatch} method.
     */
    @Category(RestTestedMethod.Patch.class)
    @Test
    public void patchMissingEntity() {

        Object patch = newObjectForPatch();
        patchEntity(true, newDocumentToInsert(), patch, HttpStatus.NOT_FOUND, true);
        patchEntity(false, newDocumentToInsert(), patch, HttpStatus.NOT_FOUND, true);
    }

    /**
     * Patching an existing entity returns a {@code 200 Ok} HTTP status code as well as the patched resource that matches the resource in the request.
     * The existing entity to patch is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
     * The patch is defined by the {@link BaseRestElasticsearchTestCase#newObjectForPatch} method.
     */
    @Category(RestTestedMethod.Patch.class)
    @Test
    public void patchValidEntity() throws IllegalAccessException {

        // The entity to patch and the patch itself
        final T entity = newExistingDocument();
        Object patch = newObjectForPatch();

        patchEntity(true, entity, patch, HttpStatus.OK, false);
        patchEntity(false, entity, patch, HttpStatus.OK, false);
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                     Tests related to entity deletion                                           */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Deleting an entity returns a {@code 405 Method Not Allowed } HTTP status code if no identifier is provided.
     */
    @Category(RestTestedMethod.Delete.class)
    @Test
    public void deleteWithoutId() {
        deleteEntity(null, HttpStatus.METHOD_NOT_ALLOWED, false, null);
    }

    /**
     * Deleting an entity returns a {@code 404 Not Found } HTTP status code if it doesn't exist.
     * The entity to delete is defined by the {@link BaseRestElasticsearchTestCase#newDocumentToInsert()} method.
     */
    @Category(RestTestedMethod.Delete.class)
    @Test
    public void deleteMissingEntity() {
        deleteEntity(getIdFieldValue(newDocumentToInsert()), HttpStatus.NOT_FOUND, false, null);
    }

    /**
     * Deleting an entity returns a {@code 400 Bad Request} HTTP status code if it is not valid.
     * Invalid entity properties are defined by the {@link BaseRestElasticsearchTestCase#getExpectedValidationErrorsOnDelete()} method.
     */
    @Category(RestTestedMethod.Delete.class)
    @Test
    public void deleteInvalidEntity() {
        getExpectedValidationErrorsOnDelete().forEach((entity, errors) -> deleteEntity(getIdFieldValue(entity), HttpStatus.BAD_REQUEST, true, errors));
    }

    /**
     * Deleting an entity returns a {@code 204 No Content } HTTP status code it exists and no validation error occurs.
     * The entity to delete is defined by the {@link BaseRestElasticsearchTestCase#newExistingDocument()} method.
     */
    @Category(RestTestedMethod.Delete.class)
    @Test
    public void deleteExistingEntity() {
        deleteEntity(getIdFieldValue(newExistingDocument()), HttpStatus.NO_CONTENT, false, null);
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                         Sub-class helper methods                                               */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Call the REST web service to create an entity.
     *
     * @param entity         Entity data to create
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError  {@code true} if an error is expected
     * @return The created entity resource
     */
    @SuppressWarnings("unused")
    protected ResponseEntity<Resource<T>> postEntity(T entity, HttpStatus expectedStatus, boolean expectedError) {
        return postEntity(entity, expectedStatus, expectedError, null);
    }

    /**
     * Call the REST web service to create an entity.
     *
     * @param entity         Entity data to create
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError  {@code true} if an error is expected
     * @param expectedErrors Expected validation errors to assert
     * @return The created entity resource
     */
    protected ResponseEntity<Resource<T>> postEntity(T entity, HttpStatus expectedStatus, boolean expectedError, List<ValidationError> expectedErrors) {

        final HttpEntity<?> httpEntity = convertToHttpEntity(entity);           // Define Hal+Json HTTP entity

        try {
            // Send a POST request
            final ResponseEntity<Resource<T>> responseEntity =
                    restTemplate.exchange(
                            getWebServiceUrl(),
                            HttpMethod.POST,
                            httpEntity,
                            getResourceType());

            assertExistingBody(expectedStatus, expectedError, responseEntity, entity);
        }

        // The POST request results in an error response
        catch (HttpStatusCodeException e) {

            assertThat(e.getStatusCode(), is(expectedStatus));  // Verify the expected HTTP status code
            if (expectedErrors != null)                         // Verify that validation errors are the expected ones
                assertThat(findValidationErrors(e).toArray(), is(expectedErrors.toArray()));
        }

        return null;
    }

    /**
     * Call the REST web service to get an entity.
     *
     * @param expectedEntity Expected entity to be found
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError  {@code true} if an error is expected
     * @return The found entity resource
     */
    @SuppressWarnings("unused")
    protected ResponseEntity<Resource<T>> getEntity(T expectedEntity, HttpStatus expectedStatus, boolean expectedError) {

        final HttpEntity<?> httpEntity = convertToHttpEntity(null);             // Define Hal+Json HTTP entity

        final Resource<T> expectedResource = convertToResource(expectedEntity);

        try {
            // Send a GET request
            final ResponseEntity<Resource<T>> responseEntity =
                    restTemplate.exchange(
                            expectedResource.getId().getHref(),
                            HttpMethod.GET,
                            httpEntity,
                            getResourceType());

            if (expectedError)  // No exception thrown whereas one is expected
                fail("Should return a " + expectedStatus.value() + " " + expectedStatus.name() + " response");

            else {  // No exception is expected, verify the expected HTTP status code and response body then return the response
                assertThat(responseEntity, is(notNullValue()));
                assertThat(responseEntity.getStatusCode(), is(expectedStatus));
                assertThat(responseEntity.getBody(), is(expectedResource));

                return responseEntity;
            }
        }

        // The POST request results in an error response
        catch (HttpStatusCodeException e) {
            assertThat(e.getStatusCode(), is(HttpStatus.NOT_FOUND));    // Verify the expected HTTP status code
        }

        return null;
    }

    /**
     * Call the REST web service to get all entities.
     *
     * @return The found entity resources
     */
    @SuppressWarnings("unused")
    protected ResponseEntity<PagedResources<Resource<T>>> getEntities() {
        return getEntities(false, null);
    }

    /**
     * Call the REST web service to get all entities.
     *
     * @param sorted {@code true} if entities are sorted
     * @return The found entity resources
     */
    protected ResponseEntity<PagedResources<Resource<T>>> getEntities(boolean sorted) {
        return getEntities(sorted, null);
    }

    /**
     * Call the REST web service to get all entities.
     *
     * @param sorted {@code true} if entities are sorted
     * @param page   {@code null} if no page is request, else a page number starting from 0
     * @return The found entity resources
     */
    protected ResponseEntity<PagedResources<Resource<T>>> getEntities(boolean sorted, Integer page) {

        final HttpEntity<?> httpEntity = convertToHttpEntity(null);             // Define Hal+Json HTTP entity

        // Get user-defined sort field and page size
        final Field sortField = getSortField();
        final int pageSize = (page == null) ? defaultPageSize : getPageSize();  // Spring Data REST always get paged resources even if

        // Build GET request parameters for sorting and paging
        final String urlParams =
                (!sorted && page == null ? "" : "?" +
                        (!sorted ? "&" : "sort=" + sortField.getName() + ",desc" +
                                (page == null ? "" : "&")) +
                        (page == null ? "" : "page=" + page + "&size=" + pageSize)
                );

        // Define the fixture for entities comparison
        final List<T> documents = sorted ?
                testClientOperations.findAllDocumentsPagedSorted(getDocumentMetaData(), sortField, (page != null) ? page : 0, pageSize) :
                testClientOperations.findAllDocumentsPaged(getDocumentMetaData(), (page != null) ? page : 0, pageSize);
        final List<Resource<T>> fixture = documents
                .stream()
                .map(this::convertToResource)
                .collect(Collectors.toList());

        // Define the fixture for metadata comparison
        final long totalDocuments = this.getTestDocumentsLoader().getLoadedDocumentCount();
        PagedResources.PageMetadata metadata = new PagedResources.PageMetadata(pageSize, (page == null ? 0 : page), totalDocuments);

        // Send a GET request
        final ResponseEntity<PagedResources<Resource<T>>> responseEntity =
                restTemplate.exchange(
                        getWebServiceUrl() + urlParams,
                        HttpMethod.GET,
                        httpEntity,
                        getPagedResourcesType());

        // Assert the entity response matches the expected one
        assertThat(responseEntity.getStatusCode(), is(HttpStatus.OK));                      // Verify HTTP status code
        assertThat(responseEntity.getBody().getMetadata(), is(metadata));                   // Verify body metadata
        assertThat(responseEntity.getBody().getContent().toArray(), is(fixture.toArray())); // Verify body content
        if (totalDocuments > documents.size())                                              // Verify body links
            assertThat(responseEntity.getBody().getLinks().toArray(), is(getPagedLanguagesLinks(sorted, page).toArray()));
        else
            assertThat(responseEntity.getBody().getLinks().toArray(), is(getLanguagesLinks().toArray()));

        return responseEntity;
    }

    /**
     * Call the REST web service to verify if an entity exists.
     *
     * @param expectedEntity Expected entity to be found
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError  {@code true} if an error is expected
     */
    protected void headEntity(T expectedEntity, HttpStatus expectedStatus, boolean expectedError) {

        final HttpEntity<?> httpEntity = convertToHttpEntity(null);             // Define Hal+Json HTTP entity

        final Resource<T> expectedResource = convertToResource(expectedEntity);

        try {
            // Send a HEAD request
            final ResponseEntity<?> responseEntity =
                    restTemplate.exchange(
                            expectedResource.getId().getHref(),
                            HttpMethod.HEAD,
                            httpEntity,
                            getResourceType());

            assertMissingBody(expectedStatus, expectedError, responseEntity);
        }

        // The POST request results in an error response
        catch (HttpStatusCodeException e) {
            assertThat(e.getStatusCode(), is(HttpStatus.NOT_FOUND));    // Verify the expected HTTP status code
        }
    }

    /**
     * Call the REST web service to head all entities.
     */
    protected void headEntities(HttpStatus expectedStatus, boolean expectedError) {

        final HttpEntity<?> httpEntity = convertToHttpEntity(null);             // Define Hal+Json HTTP entity

        // Send a HEAD request
        final ResponseEntity<?> responseEntity =
                restTemplate.exchange(
                        getWebServiceUrl(),
                        HttpMethod.HEAD,
                        httpEntity,
                        getPagedResourcesType());

        // Assert the entity response matches the expected one
        assertThat(responseEntity, is(notNullValue()));
        assertThat(responseEntity.getStatusCode(), is(expectedStatus));                     // Verify HTTP status code
        assertThat(responseEntity.getBody(), is(nullValue()));
    }

    /**
     * Call the REST web service to update.
     *
     * @param entity         Entity data to update
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError  {@code true} if an error is expected
     * @return The updated entity resource
     */
    @SuppressWarnings("unused")
    protected ResponseEntity<Resource<T>> putEntity(T entity, HttpStatus expectedStatus, boolean expectedError) {
        return putEntity(entity, expectedStatus, expectedError, null);
    }

    /**
     * Call the REST web service to update.
     *
     * @param entity         Entity data to update
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError  {@code true} if an error is expected
     * @param expectedErrors Expected validation errors to assert
     * @return The updated entity resource
     */
    protected ResponseEntity<Resource<T>> putEntity(T entity, HttpStatus expectedStatus, boolean expectedError, List<ValidationError> expectedErrors) {
        return putEntity(entity, expectedStatus, expectedError, expectedErrors, false);
    }

    /**
     * Call the REST web service to update.
     *
     * @param entity         Entity data to update
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError  {@code true} if an error is expected
     * @param expectedErrors Expected validation errors to assert
     * @param emptyBody      To send an empty body
     * @return The updated entity resource
     */
    protected ResponseEntity<Resource<T>> putEntity(T entity, HttpStatus expectedStatus, boolean expectedError, List<ValidationError> expectedErrors, boolean emptyBody) {

        // Define Hal+Json HTTP entity
        final HttpEntity<?> httpEntity = convertToHttpEntity(entity == null || emptyBody ? null : entity);

        // Identifier of the entity resource to update
        final ID id = (entity != null) ? getIdFieldValue(entity) : null;

        try {
            // Send a PUT request
            final ResponseEntity<Resource<T>> responseEntity =
                    restTemplate.exchange(
                            getWebServiceUrl() + (id == null ? "" : "/" + id),
                            HttpMethod.PUT,
                            httpEntity,
                            getResourceType());

            assertExistingBody(expectedStatus, expectedError, responseEntity, entity);
        }

        // The PUT request results in an error response
        catch (HttpStatusCodeException e) {

            assertThat(e.getStatusCode(), is(expectedStatus));  // Verify the expected HTTP status code
            if (expectedErrors != null)                         // Verify that validation errors are the expected ones
                assertThat(findValidationErrors(e).toArray(), is(expectedErrors.toArray()));
        }

        return null;
    }

    /**
     * Call the REST web service to partially update.
     *
     * @param merge          {@code true} to use JSON Merge Patch format and {@code false} to use JSON Patch format
     * @param entity         Entity data to patch
     * @param patch          Object with some getters matching the {@link T} entity getters, used for patching these fields
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError  {@code true} if an error is expected
     * @return The updated entity resource
     */
    protected ResponseEntity<Resource<T>> patchEntity(boolean merge, T entity, Object patch, HttpStatus expectedStatus, boolean expectedError) {
        return patchEntity(merge, entity, patch, expectedStatus, expectedError, null, false);
    }

    /**
     * Call the REST web service to partially update with JSON Patch method.
     *
     * @param merge          {@code true} to use JSON Merge Patch format and {@code false} to use JSON Patch format
     * @param entity         Entity data to patch
     * @param patch          Object with some getters matching the {@link T} entity getters, used for patching these fields
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError  {@code true} if an error is expected
     * @param expectedErrors Expected validation errors to assert
     * @param emptyBody      To send an empty body
     * @return The updated entity resource
     */
    protected ResponseEntity<Resource<T>> patchEntity(boolean merge, T entity, Object patch, HttpStatus expectedStatus, boolean expectedError, List<ValidationError> expectedErrors, boolean emptyBody) {

        final T targetEntity;

        // Define Patch+Json HTTP entity
        final HttpEntity<?> httpEntity;
        if (entity == null || emptyBody) {
            // Set empty body to the HTTP entity
            httpEntity = convertToHttpEntity(null, merge ? MERGE_PATCH_JSON_UTF8 : JSON_PATCH_JSON_UTF8);
            targetEntity = null;
        } else {
            final ObjectMapper mapper = new ObjectMapper(); // JSON converter

            // Convert the object for patching to a JSON merge patch
            JsonNode jsonMerge = mapper.valueToTree(patch);

            // Get entity to patch in JSON format
            final JsonNode jsonSource = mapper.valueToTree(entity);

            // Get the target entity the PATCH request must match by applying the JSON merge patch to the source entity
            final JsonNode jsonTarget;
            try {
                jsonTarget = JsonMergePatch.fromJson(jsonMerge).apply(jsonSource);
            } catch (JsonPatchException e) {
                throw new RuntimeException(e);
            }
            try {
                targetEntity = mapper.treeToValue(jsonTarget, getDocumentMetaData().getDocumentClass());
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            // Get a patch operation in Json merge patch or Json patch format
            final JsonNode jsonPatchNode = merge ? jsonMerge : JsonDiff.asJson(jsonSource, jsonTarget);

            // Set the patch value to the HTTP entity's body
            httpEntity = convertToHttpEntity(jsonPatchNode.toString(), merge ? MERGE_PATCH_JSON_UTF8 : JSON_PATCH_JSON_UTF8);
        }

        // Identifier of the entity resource to update
        final ID id = (entity != null) ? getIdFieldValue(entity) : null;

        try {
            // Send a PATCH request
            final ResponseEntity<Resource<T>> responseEntity =
                    restTemplate.exchange(
                            getWebServiceUrl() + (id == null ? "" : "/" + id),
                            HttpMethod.PATCH,
                            httpEntity,
                            getResourceType());

            assertExistingBody(expectedStatus, expectedError, responseEntity, targetEntity);
        }
        // The PATCH request results in an error response
        catch (HttpStatusCodeException e) {

            assertThat(e.getStatusCode(), is(expectedStatus));  // Verify the expected HTTP status code
            if (expectedErrors != null)                         // Verify that validation errors are the expected ones
                assertThat(findValidationErrors(e).toArray(), is(expectedErrors.toArray()));
        }

        return null;
    }

    /**
     * Call the REST web service to delete.
     *
     * @param id             Entity identifier to delete
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError  {@code true} if an error is expected
     * @param expectedErrors Expected validation errors to assert
     */
    protected void deleteEntity(ID id, HttpStatus expectedStatus, boolean expectedError, List<ValidationError> expectedErrors) {

        final HttpEntity<?> httpEntity = convertToHttpEntity(null);             // Define Hal+Json HTTP entity

        try {
            // Send a DELETE request
            final ResponseEntity<?> responseEntity =
                    restTemplate.exchange(
                            getWebServiceUrl() + (id == null ? "" : "/" + id),
                            HttpMethod.DELETE,
                            httpEntity,
                            getResourceType());

            assertMissingBody(expectedStatus, expectedError, responseEntity);
        }

        // The DELETE request results in an error response
        catch (HttpStatusCodeException e) {

            assertThat(e.getStatusCode(), is(expectedStatus));  // Verify the expected HTTP status code
            if (expectedErrors != null)                         // Verify that validation errors are the expected ones
                assertThat(findValidationErrors(e).toArray(), is(expectedErrors.toArray()));
        }
    }

    /**
     * Convert an entity to an HTTP entity with Hal+Json content type.
     *
     * @param entity The entity to convert
     * @return The converted entity
     */
    protected HttpEntity<?> convertToHttpEntity(Object entity) {
        return convertToHttpEntity(entity, RestMediaTypes.HAL_JSON);
    }

    /**
     * Convert an entity to an HTTP entity with the specified content type.
     *
     * @param entity      The entity to convert
     * @param contentType The content type to set
     * @return The converted entity
     */
    protected HttpEntity<?> convertToHttpEntity(Object entity, MediaType contentType) {

        // Define headers and body
        return new HttpEntity<>(entity, new HttpHeaders() {{
            setContentType(contentType);
        }});
    }

    /**
     * Convert an entity to a resource containing the entity with HATEOAS links.
     *
     * @param entity The language to convert
     * @return The resource containing a language
     */
    protected Resource<T> convertToResource(final T entity) {

        // Get the identifier value
        String id;
        try {
            id = getDocumentMetaData().getDocumentIdField().get(entity).toString();
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        // Convert entity into resource
        Resource<T> result = new Resource<>(entity);
        result.add(new Link(getWebServiceUrl().toString() + "/" + id));
        result.add(new Link(getWebServiceUrl().toString() + "/" + id, getDocumentMetaData().getDocumentClass().getSimpleName().toLowerCase()));

        // Set the resource identifier to "null" : the identifier isn't sent in the entity response
        getDocumentMetaData().getDocumentIdField().setAccessible(true);
        try {
            getDocumentMetaData().getDocumentIdField().set(result.getContent(), null);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    /**
     * Compute the total number of pages.
     *
     * @return The total number of pages
     */
    protected int getTotalPages() {

        long totalElements = this.getTestDocumentsLoader().getLoadedDocumentCount();
        int pageSize = getPageSize();

        return (int) Math.ceil((double) totalElements / (double) pageSize);
    }

    /**
     * Get expected HATEOAS links when requesting language resources.
     *
     * @return A list of HATEOAS links
     */
    protected List<Link> getLanguagesLinks() {

        return Arrays.asList(
                new Link(getWebServiceUrl().toString()),
                new Link(getWebServiceUrl(true).toString(), "profile"));
    }

    /**
     * Get expected HATEOAS links when requesting paged language resources
     *
     * @param sorted {@code true} if entities are sorted
     * @param page   {@code null} if no page is request, else a page number starting from 0
     * @return A list of HATEOAS links
     */
    protected List<Link> getPagedLanguagesLinks(boolean sorted, Integer page) {

        final int pageNo = (page == null) ? 0 : page;
        final String fieldToSortBy = getSortField().getName();
        final int pageSize = (page == null) ? defaultPageSize : getPageSize();
        final long documentCount = this.getTestDocumentsLoader().getLoadedDocumentCount();
        final long lastPage = documentCount / pageSize - (documentCount % pageSize == 0 ? 1 : 0);

        List<Link> links = new ArrayList<>();
        links.add(new Link(getWebServiceUrl().toString() + "?page=0&size=" + pageSize + (sorted ? "&sort=" + fieldToSortBy + ",desc" : ""), "first"));
        if (pageNo > 0)
            links.add(new Link(getWebServiceUrl().toString() + "?page=" + (pageNo - 1) + "&size=" + pageSize + (sorted ? "&sort=" + fieldToSortBy + ",desc" : ""), "prev"));
        links.add(new Link(getWebServiceUrl().toString()));
        if (pageNo < lastPage)
            links.add(new Link(getWebServiceUrl().toString() + "?page=" + (pageNo + 1) + "&size=" + pageSize + (sorted ? "&sort=" + fieldToSortBy + ",desc" : ""), "next"));
        links.add(new Link(getWebServiceUrl().toString() + "?page=" + lastPage + "&size=" + pageSize + (sorted ? "&sort=" + fieldToSortBy + ",desc" : ""), "last"));
        links.add(new Link(getWebServiceUrl(true).toString(), "profile"));

        return links;
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                          Private helper methods                                                */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Get URL of the web service to test.
     *
     * @return URL of the web service to test
     */
    private URI getWebServiceUrl() {
        return getWebServiceUrl(false);
    }

    /**
     * Get URL of the web service to test.
     *
     * @param profile Indicate if a profile URL is returned
     * @return URL of the web service to test
     */
    private URI getWebServiceUrl(boolean profile) {

        final String webContext = "/" + getDocumentMetaData().getDocumentClass().getSimpleName().toLowerCase() + "s";

        try {
            return new URI("http", null, "localhost", serverPort, (profile ? "/profile" : "") + webContext, null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid server URI", e);
        }
    }

    /**
     * Find validation errors from the HTTP exception body.
     *
     * @param httpException HTTP exception that contains validation errors
     * @return The list of validation errors found
     */
    private List<ValidationError> findValidationErrors(HttpStatusCodeException httpException) {

        ObjectMapper mapper = new ObjectMapper();
        try {
            // The "errors" child node contains all validation errors
            JsonNode errorsNode = mapper.readTree(httpException.getResponseBodyAsString()).get("errors");

            // Map each error to a ValidationError object
            TypeReference<ArrayList<ValidationError>> typeRef = new TypeReference<ArrayList<ValidationError>>() {
            };
            return mapper.readValue(errorsNode.traverse(), typeRef);

        } catch (IOException ioe) {

            fail("Could not convert response body into JSON");
            return new ArrayList<>();
        }
    }

    /**
     * Assert the expected status code is verified and the response body is missing.
     *
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError  {@code true} if an error is expected
     * @param responseEntity Entity response to inspect
     */
    private void assertMissingBody(final HttpStatus expectedStatus, boolean expectedError, final ResponseEntity<?> responseEntity) {

        if (expectedError)  // No exception thrown whereas one is expected
            fail("Should return a " + expectedStatus.value() + " " + expectedStatus.name() + " response");

        else {  // No exception is expected, verify the expected HTTP status code and response body then return the response
            assertThat(responseEntity, is(notNullValue()));
            assertThat(responseEntity.getStatusCode(), is(expectedStatus));
            assertThat(responseEntity.getBody(), is(nullValue()));
        }
    }

    /**
     * Assert the expected status code is verified and the response body matches the expected one.
     *
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError  {@code true} if an error is expected
     * @param responseEntity Entity response to inspect
     * @param expectedEntity Entity that must match the response
     */
    private void assertExistingBody(final HttpStatus expectedStatus, boolean expectedError, final ResponseEntity<?> responseEntity, final T expectedEntity) {

        if (expectedError)  // No exception thrown whereas one is expected
            fail("Should return a " + expectedStatus.value() + " " + expectedStatus.name() + " response");

        else {  // No exception is expected, verify the expected HTTP status code and return the response
            assertThat(responseEntity, is(notNullValue()));
            assertThat(responseEntity.getStatusCode(), is(expectedStatus));
            assertThat(responseEntity.getBody(), is(convertToResource(expectedEntity)));
        }
    }
}
