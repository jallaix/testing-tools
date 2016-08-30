package info.jallaix.spring.data.es.test;

/**
 * Enumeration of the testable methods.
 */
public interface RestTestedMethod {

    public interface Create extends RestTestedMethod {}
    public interface Update extends RestTestedMethod {}
    public interface FindAll extends RestTestedMethod {}
    public interface FindAllPageable extends RestTestedMethod {}
    public interface FindAllSorted extends RestTestedMethod {}
    public interface FindOne extends RestTestedMethod {}
    public interface DeleteAll extends RestTestedMethod {}
    public interface DeleteById extends RestTestedMethod {}
}
