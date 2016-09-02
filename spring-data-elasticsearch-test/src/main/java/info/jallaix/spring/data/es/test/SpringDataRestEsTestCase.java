package info.jallaix.spring.data.es.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * <p>
 * Test class for the Spring Data REST Elasticsearch module.
 * <p>
 * It supports data initialization thanks to the <a href="https://github.com/tlrx/elasticsearch-test">elasticsearch-test framework</a>.
 */
@SuppressWarnings("unused")
public abstract class SpringDataRestEsTestCase<T, ID extends Serializable, R extends ElasticsearchRepository<T, ID>> extends SpringDataEsTestCase<T, ID, R> {

    /**
     * Random server port
     */
    @Value("${local.server.port}")
    private int serverPort;

    /**
     * Ability to get the current test name
     */
    @Rule
    public TestName name = new TestName();

    /**
     * Test client operations
     */
    @SuppressWarnings("SpringJavaAutowiredMembersInspection")
    @Autowired
    private TestClientOperations testClientOperations;


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                           Ignored tests system                                                 */
    /*----------------------------------------------------------------------------------------------------------------*/

    @Before
    public void selectTests() {
        Assume.assumeTrue(isTestPlayed(testedMethods));
    }

    /**
     * Set of tested methods
     */
    private Set<Class<?>> testedMethods;

    /**
     * Constructor with list of methods to test
     *
     * @param methods Methods to test
     */
    @SafeVarargs
    @SuppressWarnings("unused")
    public SpringDataRestEsTestCase(Class<? extends RestTestedMethod>... methods) {

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
    /*                                    Tests related to language creation                                          */
    /*----------------------------------------------------------------------------------------------------------------*/

    @Category(RestTestedMethod.Create.class)
    @Test
    @SuppressWarnings("unused")
    public void createInvalidEntity() {

        // Test null language
        postEntity(null, HttpStatus.BAD_REQUEST, true, null);
    }


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
                            new TypeReferences.ResourceType<T>() {});


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
     * @param linkId HATEOAS link to get the entity
     * @param expectedStatus Expected HTTP status to assert
     * @param expectedError {@code true} if an error is expected
     * @return The found entity resource
     */
    @SuppressWarnings("unused")
    protected ResponseEntity<Resource<T>> getEntity(Link linkId, HttpStatus expectedStatus, boolean expectedError) {

        try {
            // Send a GET request
            ResponseEntity<Resource<T>> responseEntity =
                    getHalRestTemplate().exchange(
                            linkId.getHref(),
                            HttpMethod.GET,
                            null,
                            new TypeReferences.ResourceType<T>() {});

            if (expectedError)  // No exception thrown whereas one is expected
                fail("Should return a " + expectedStatus.value() + " " + expectedStatus.name() + " response");

            else {  // No exception is expected, verify the expected HTTP status code and return the response
                assertThat(responseEntity.getStatusCode(), is(expectedStatus));
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
        List<T> documents;
        String urlParams = "?sort=" + sortField.getName() + ",desc";
        if (page != null)
            urlParams += "&page=" + page + "&size=" + pageSize;

        // Define the fixture for comparison of entities
        if (page != null)
            documents = testClientOperations.findAllDocumentsByPage(getDocumentMetadata(), documentClass, sortField, page, pageSize);
        else
            documents = testClientOperations.findAllDocumentsSorted(getDocumentMetadata(), documentClass, sortField);
        List<Resource<T>> fixture = documents
                .stream()
                .map(this::convertToResource)
                .collect(Collectors.toList());

        // Send a GET request
        ResponseEntity<PagedResources<Resource<T>>> responseEntity =
                getHalRestTemplate().exchange(
                        getWebServiceUrl() + urlParams,
                        HttpMethod.GET,
                        null,
                        new TypeReferences.PagedResourcesType<Resource<T>>() {
                        });

        // Verify the expected HTTP status code and body content
        assertThat(responseEntity.getStatusCode(), is(HttpStatus.OK));
        assertThat(responseEntity.getBody().getContent().toArray(), is(fixture.toArray()));
        if (page != null)
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
     * @param expectedErrors Expected validation errors to assert
     * @return The updated entity resource
     */
    @SuppressWarnings("unused")
    protected ResponseEntity<Resource<T>> updateLanguage(String id, T entity, HttpStatus expectedStatus, boolean expectedError, List<ValidationError> expectedErrors) {

        // Define headers and body
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.parseMediaType("application/hal+json"));
        HttpEntity<T> httpEntity = new HttpEntity<>(entity, httpHeaders);

        try {
            // Send a POST request
            ResponseEntity<Resource<T>> responseEntity =
                    getHalRestTemplate().exchange(
                            getWebServiceUrl() + "/" + id,
                            HttpMethod.PUT,
                            httpEntity,
                            new TypeReferences.ResourceType<T>() {});

            if (expectedError)  // No exception thrown whereas one is expected
                fail("Should return a " + expectedStatus.value() + " " + expectedStatus.name() + " response");

            else {  // No exception is expected, verify the expected HTTP status code and return the response
                assertThat(responseEntity.getStatusCode(), is(expectedStatus));
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
    protected Resource<T> convertToResource(T entity) {

        String id = "undefined";
        try {
            id = documentIdField.get(entity).toString();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        Resource<T> result = new Resource<>(entity);
        result.add(new Link(getWebServiceUrl().toString() + "/" + id));
        result.add(new Link(getWebServiceUrl().toString() + "/" + id, documentClass.getSimpleName().toLowerCase()));

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
    protected List<Link> getPagedLanguagesLinks(int page) {

        final String fieldToSortBy = getSortField().getName();
        final int pageSize = getPageSize();
        final long documentCount = testClientOperations.countDocuments(getDocumentMetadata());
        final long lastPage = documentCount / pageSize - (documentCount % pageSize == 0 ? 1 : 0);

        List<Link> links = new ArrayList<>();
        links.add(new Link(getWebServiceUrl().toString() + "?page=0&size=" + pageSize + "&sort=" + fieldToSortBy + ",desc", "first"));
        if (page > 0)
            links.add(new Link(getWebServiceUrl().toString() + "?page=" + (page - 1) + "&size=" + pageSize + "&sort=" + fieldToSortBy + ",desc", "prev"));
        links.add(new Link(getWebServiceUrl().toString()));
        if (page < lastPage)
            links.add(new Link(getWebServiceUrl().toString() + "?page=" + (page + 1) + "&size=" + pageSize + "&sort=" + fieldToSortBy + ",desc", "next"));
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

        final String webContext = "/" + documentClass.getSimpleName().toLowerCase() + "s";

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
