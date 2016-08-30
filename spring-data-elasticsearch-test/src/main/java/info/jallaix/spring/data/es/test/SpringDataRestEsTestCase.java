package info.jallaix.spring.data.es.test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.hateoas.ResourceSupport;
import org.springframework.hateoas.hal.Jackson2HalModule;
import org.springframework.hateoas.mvc.TypeReferences;
import org.springframework.http.*;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Created by JAX on 30/08/2016.
 */
public abstract class SpringDataRestEsTestCase<T, ID extends Serializable, R extends ElasticsearchRepository<T, ID>, TR extends ResourceSupport> extends SpringDataEsTestCase<T, ID, R> {

    @Value("${local.server.port}")
    private int serverPort;

    @Rule
    public TestName name = new TestName();


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
    public void createInvalidEntity() {

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.parseMediaType("application/hal+json"));
        HttpEntity<T> httpEntityNull = new HttpEntity<>(null, httpHeaders);
        try {
            getHalRestTemplate().exchange(
                    getWebServiceUrl(),
                    HttpMethod.POST,
                    httpEntityNull,
                    new TypeReferences.ResourceType<TR>() {});
            fail("Should return a 400 BAD REQUEST response");
        }
        catch (HttpStatusCodeException e) {

            assertThat(e.getStatusCode(), is(HttpStatus.BAD_REQUEST));
        }
    }


    /*----------------------------------------------------------------------------------------------------------------*/
    /*                                              Helper methods                                                    */
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
        try {
            return new URI("http", null, "localhost", serverPort, (profile ? "/profile" : "") + "/languages", null, null);
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
}
