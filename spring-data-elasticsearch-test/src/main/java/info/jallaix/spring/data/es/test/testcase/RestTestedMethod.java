package info.jallaix.spring.data.es.test.testcase;

/**
 * Enumeration of the REST testable methods.
 */
public interface RestTestedMethod {

    public interface Create extends RestTestedMethod {}
    public interface FindOne extends RestTestedMethod {}
    public interface FindAll extends RestTestedMethod {}
    public interface FindAllPageable extends RestTestedMethod {}
    public interface Exist extends RestTestedMethod {}
    public interface Update extends RestTestedMethod {}
    public interface Patch extends RestTestedMethod {}
    public interface Delete extends RestTestedMethod {}
}
