package info.jallaix.spring.data.es.test.testcase;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.jallaix.spring.data.es.test.bean.ValidationError;
import info.jallaix.spring.data.es.test.util.TestClientOperations;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.PagedResources;
import org.springframework.hateoas.Resource;
import org.springframework.hateoas.hal.Jackson2HalModule;
import org.springframework.hateoas.mvc.TypeReferences;
import org.springframework.http.*;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * <p>
 * Test class for the Spring Data REST Elasticsearch module.
 * <p>
 * It supports data initialization thanks to the <a href="https://github.com/tlrx/elasticsearch-test">elasticsearch-test framework</a>.
 */
@SuppressWarnings("unused")
public abstract class BaseRestElasticsearchTestCase<T, ID extends Serializable, R extends ElasticsearchRepository<T, ID>> extends BaseElasticsearchTestCase<T, ID, R> {

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
                    RestTestedMethod.FindAllSorted.class,
                    RestTestedMethod.FindOne.class,
                    RestTestedMethod.DeleteAll.class,
                    RestTestedMethod.DeleteById.class));
        else
            testedMethods = new HashSet<>(Arrays.asList(methods));
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                             Abstract methods                                                   */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Get the resource type for JSON/Object mapping.
     * @return The resource type
     */
    protected abstract TypeReferences.ResourceType<T> getResourceType();

    /**
     * Get the paged resources type for JSON/Object mapping.
     * @return The paged resources type
     */
    protected abstract TypeReferences.PagedResourcesType<Resource<T>> getPagedResourcesType();

    /**
     * Return a map of entities linked to a list of expected validation errors.
     * Each entity must hold a set of properties that causes some validation errors to occur.
     * @return The map of entities linked to a list of expected validation errors
     */
    protected abstract Map<T, List<ValidationError>> getExpectedValidationErrors();


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
     * Invalid entity properties are defined by the {@link BaseRestElasticsearchTestCase#getExpectedValidationErrors} method.
     */
    @Category(RestTestedMethod.Create.class)
    @Test
    public void createInvalidEntity() {
        getExpectedValidationErrors().forEach((entity, errors) -> postEntity(entity, HttpStatus.BAD_REQUEST, true, errors));
    }

    /**
     * Creating an entity returns a {@code 409 Conflict} HTTP status code if it already exists.
     */
    @Category(RestTestedMethod.Create.class)
    @Test
    public void createDuplicateEntity() {
        postEntity(newDocumentToUpdate(), HttpStatus.CONFLICT, true);
    }

    /**
     * Creating an entity returns a {@code 201 Created} HTTP status code if the entry doesn't already exist and the entity argument is valid.
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
     */
    @Category(RestTestedMethod.FindOne.class)
    @Test
    public void findOneMissingEntity() { getEntity(newDocumentToInsert(), HttpStatus.NOT_FOUND, true); }

    /**
     * Getting an entity returns this entity in HATEOAS format and a {@code 200 Ok} HTTP status code if the entity is found.
     */
    @Category(RestTestedMethod.FindOne.class)
    @Test
    public void findOneExistingEntity() { getEntity(newExistingDocument(), HttpStatus.OK, false); }

    /**
     * Getting all entities sorted returns these entities in HATEOAS format and a {@code 200 Ok} HTTP status code.
     */
    @Category(RestTestedMethod.FindAllSorted.class)
    @Test
    public void findAllEntitiesSorted() {
        getEntities();
    }

    /**
     * Getting all entities sorted and paged returns these entities in HATEOAS format and a {@code 200 Ok} HTTP status code.
     */
    @Category(RestTestedMethod.FindAllPageable.class)
    @Test
    public void findAllPagedEntitiesSorted() {

        getEntities(0);
        getEntities(1);
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                     Tests related to language update                                           */
    /*----------------------------------------------------------------------------------------------------------------*/

    /*
     * Updating an entity returns a {@code 405 Method Not Allowed} HTTP status code if no identifier is provided.
     */
    @Category(RestTestedMethod.Update.class)
    @Test
    public void updateEntityWithoutId() { putEntity(null, null, HttpStatus.METHOD_NOT_ALLOWED, true); }

    /**
     * Updating an entity returns a {@code 400 Bad Request} HTTP status code if no entity is provided.
     */
    @Category(RestTestedMethod.Update.class)
    @Test
    public void updateInvalidEntity() {
        putEntity(getIdFieldValue(newExistingDocument()), null, HttpStatus.BAD_REQUEST, true);
    }

    /**
     * Updating an entity returns {@code 404 Not Found} HTTP status code if there is no existing language to update.
     */
    @Category(RestTestedMethod.Update.class)
    @Test
    public void updateMissingEntity() {

        T document = newDocumentToInsert();
        putEntity(getIdFieldValue(document), document, HttpStatus.NOT_FOUND, true);
    }

    /**
     * Updating an existing entity returns a {@code 200 Ok} HTTP status code as well as the updated resource that matches the resource in the request.
     */
    @Category(RestTestedMethod.Update.class)
    @Test
    public void updateValidEntity() {

        T expectedEntity = newDocumentToUpdate();
        putEntity(getIdFieldValue(expectedEntity), expectedEntity, HttpStatus.OK, false);
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                    Tests related to language deletion                                          */
    /*----------------------------------------------------------------------------------------------------------------*/

    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                         Sub-class helper methods                                               */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Call the REST web service to create an entity
     * @param entity Entity data to create
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError {@code true} if an error is expected
     * @return The created entity resource
     */
    @SuppressWarnings("unused")
    protected ResponseEntity<Resource<T>> postEntity(T entity, HttpStatus expectedStatus, boolean expectedError) {
        return postEntity(entity, expectedStatus, expectedError, null);
    }

    /**
     * Call the REST web service to create an entity
     * @param entity Entity data to create
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError {@code true} if an error is expected
     * @param expectedErrors Expected validation errors to assert
     * @return The created entity resource
     */
    protected ResponseEntity<Resource<T>> postEntity(T entity, HttpStatus expectedStatus, boolean expectedError, List<ValidationError> expectedErrors) {

        // Define headers and body
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.parseMediaType("application/hal+json"));
        HttpEntity<T> httpEntity = new HttpEntity<>(entity, httpHeaders);

        try {
            // Send a POST request
            ResponseEntity<Resource<T>> responseEntity =
                    getHalRestTemplate().exchange(
                            getWebServiceUrl(),
                            HttpMethod.POST,
                            httpEntity,
                            getResourceType());


            if (expectedError)  // No exception thrown whereas one is expected
                fail("Should return a " + expectedStatus.value() + " " + expectedStatus.name() + " response");

            else {  // No exception is expected, verify the expected HTTP status code and return the response
                assertThat(responseEntity.getStatusCode(), is(expectedStatus));
                return responseEntity;
            }
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
     * Call the REST web service to get an entity
     * @param expectedEntity Expected entity to be found
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError {@code true} if an error is expected
     * @return The found entity resource
     */
    @SuppressWarnings("unused")
    protected ResponseEntity<Resource<T>> getEntity(T expectedEntity, HttpStatus expectedStatus, boolean expectedError) {

        Resource<T> expectedResource = convertToResource(expectedEntity);

        try {
            // Send a GET request
            ResponseEntity<Resource<T>> responseEntity =
                    getHalRestTemplate().exchange(
                            expectedResource.getId().getHref(),
                            HttpMethod.GET,
                            null,
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
     * Call the REST web service to get all entities
     * @return The found entity resources
     */
    @SuppressWarnings("unused")
    protected ResponseEntity<PagedResources<Resource<T>>> getEntities() { return getEntities(null); }

    /**
     * Call the REST web service to get all entities
     * @param page {@code null} if no page is request, else a page number starting from 0
     * @return The found entity resources
     */
    protected ResponseEntity<PagedResources<Resource<T>>> getEntities(Integer page) {

        // Get user-defined sort field and page size
        final Field sortField = getSortField();
        final int pageSize = getPageSize();

        // Build GET request parameters for sorting and paging
        final String urlParams =
                "?sort=" + sortField.getName() + ",desc" +
                (page == null ? "" : "&page=" + page + "&size=" + pageSize);

        // Define the fixture for comparison of entities
        final List<T> documents = page != null ?
            testClientOperations.findAllDocumentsByPage(getDocumentMetadata(), getDocumentClass(), sortField, page, pageSize) :
            testClientOperations.findAllDocumentsSorted(getDocumentMetadata(), getDocumentClass(), sortField, defaultPageSize);
        final List<Resource<T>> fixture = documents
                .stream()
                .map(this::convertToResource)
                .collect(Collectors.toList());
        final long totalDocuments = this.getTestDocumentsLoader().getLoadedDocumentCount();

        // Send a GET request
        final ResponseEntity<PagedResources<Resource<T>>> responseEntity =
                getHalRestTemplate().exchange(
                        getWebServiceUrl() + urlParams,
                        HttpMethod.GET,
                        null,
                        getPagedResourcesType());

        // Verify the expected HTTP status code and body content
        assertThat(responseEntity.getStatusCode(), is(HttpStatus.OK));
        assertThat(responseEntity.getBody().getContent().toArray(), is(fixture.toArray()));
        if (totalDocuments > documents.size())
            assertThat(responseEntity.getBody().getLinks().toArray(), is(getPagedLanguagesLinks(page).toArray()));
        else
            assertThat(responseEntity.getBody().getLinks().toArray(), is(getLanguagesLinks().toArray()));

        return responseEntity;
    }

    /**
     * Call the REST web service to update
     * @param id Identifier of the entity resource to update
     * @param entity Entity data to update
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError {@code true} if an error is expected
     * @return The updated entity resource
     */
    @SuppressWarnings("unused")
    protected ResponseEntity<Resource<T>> putEntity(ID id, T entity, HttpStatus expectedStatus, boolean expectedError) {
        return putEntity(id, entity, expectedStatus, expectedError, null);
    }

    /**
     * Call the REST web service to update
     * @param id Identifier of the entity resource to update
     * @param entity Entity data to update
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError {@code true} if an error is expected
     * @param expectedErrors Expected validation errors to assert
     * @return The updated entity resource
     */
    protected ResponseEntity<Resource<T>> putEntity(ID id, T entity, HttpStatus expectedStatus, boolean expectedError, List<ValidationError> expectedErrors) {

        // Define headers and body
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.parseMediaType("application/hal+json"));
        HttpEntity<T> httpEntity = new HttpEntity<>(entity, httpHeaders);

        try {
            // Send a PUT request
            ResponseEntity<Resource<T>> responseEntity =
                    getHalRestTemplate().exchange(
                            getWebServiceUrl() + (id == null ? "" : "/" + id),
                            HttpMethod.PUT,
                            httpEntity,
                            getResourceType());

            if (expectedError)  // No exception thrown whereas one is expected
                fail("Should return a " + expectedStatus.value() + " " + expectedStatus.name() + " response");

            else {  // No exception is expected, verify the expected HTTP status code and return the response
                assertThat(responseEntity, is(notNullValue()));
                assertThat(responseEntity.getStatusCode(), is(expectedStatus));
                assertThat(responseEntity.getBody(), is(convertToResource(entity)));
                return responseEntity;
            }
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
     * Convert an entity to a resource containing the entity with HATEOAS links
     *
     * @param entity The language to convert
     * @return The resource containing a language
     */
    protected Resource<T> convertToResource(final T entity) {

        // Get the identifier value
        String id;
        try {
            id = getDocumentIdField().get(entity).toString();
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        // Convert entity into resource
        Resource<T> result = new Resource<>(entity);
        result.add(new Link(getWebServiceUrl().toString() + "/" + id));
        result.add(new Link(getWebServiceUrl().toString() + "/" + id, getDocumentClass().getSimpleName().toLowerCase()));

        // Set the resource identifier to "null" : the identifier isn't sent in the entity response
        getDocumentIdField().setAccessible(true);
        try {
            getDocumentIdField().set(result.getContent(), null);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    /**
     * Get expected HATEOAS links when requesting language resources
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
     * @return A list of HATEOAS links
     */
    protected List<Link> getPagedLanguagesLinks(Integer page) {

        final int pageNo = (page == null) ? 0 : page;
        final String fieldToSortBy = getSortField().getName();
        final int pageSize = (page == null) ? defaultPageSize : getPageSize();
        final long documentCount = testClientOperations.countDocuments(getDocumentMetadata());
        final long lastPage = documentCount / pageSize - (documentCount % pageSize == 0 ? 1 : 0);

        List<Link> links = new ArrayList<>();
        links.add(new Link(getWebServiceUrl().toString() + "?page=0&size=" + pageSize + "&sort=" + fieldToSortBy + ",desc", "first"));
        if (pageNo > 0)
            links.add(new Link(getWebServiceUrl().toString() + "?page=" + (pageNo - 1) + "&size=" + pageSize + "&sort=" + fieldToSortBy + ",desc", "prev"));
        links.add(new Link(getWebServiceUrl().toString()));
        if (pageNo < lastPage)
            links.add(new Link(getWebServiceUrl().toString() + "?page=" + (pageNo + 1) + "&size=" + pageSize + "&sort=" + fieldToSortBy + ",desc", "next"));
        links.add(new Link(getWebServiceUrl().toString() + "?page=" + lastPage + "&size=" + pageSize + "&sort=" + fieldToSortBy + ",desc", "last"));
        links.add(new Link(getWebServiceUrl(true).toString(), "profile"));

        return links;
    }

    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                          Private helper methods                                                */
    /*----------------------------------------------------------------------------------------------------------------*/

    /**
     * Get URL of the web service to test
     * @return URL of the web service to test
     */
    private URI getWebServiceUrl() {
        return getWebServiceUrl(false);
    }

    /**
     * Get URL of the web service to test
     * @param profile Indicate if a profile URL is returned
     * @return URL of the web service to test
     */
    private URI getWebServiceUrl(boolean profile) {

        final String webContext = "/" + getDocumentClass().getSimpleName().toLowerCase() + "s";

        try {
            return new URI("http", null, "localhost", serverPort, (profile ? "/profile" : "") + webContext, null, null);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid server URI", e);
        }
    }

    /**
     * Get a HAL REST template
     * @return A HAL REST template
     */
    private RestTemplate getHalRestTemplate() {

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.registerModule(new Jackson2HalModule());

        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setSupportedMediaTypes(MediaType.parseMediaTypes("application/hal+json"));
        converter.setObjectMapper(mapper);

        return new RestTemplate(Collections.<HttpMessageConverter<?>> singletonList(converter));
    }

    /**
     * Find validation errors from the HTTP exception body
     * @param httpException HTTP exception that contains validation errors
     * @return The list of validation errors found
     */
    private List<ValidationError> findValidationErrors(HttpStatusCodeException httpException) {

        ObjectMapper mapper = new ObjectMapper();
        try {
            // The "errors" child node contains all validation errors
            JsonNode errorsNode = mapper.readTree(httpException.getResponseBodyAsString()).get("errors");

            // Map each error to a ValidationError object
            TypeReference<ArrayList<ValidationError>> typeRef = new TypeReference<ArrayList<ValidationError>>() {};
            return mapper.readValue(errorsNode.traverse(), typeRef);

        } catch (IOException ioe) {

            fail("Could not convert response body into JSON");
            return new ArrayList<>();
        }
    }
}
