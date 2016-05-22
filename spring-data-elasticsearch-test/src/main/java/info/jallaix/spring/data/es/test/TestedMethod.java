package info.jallaix.spring.data.es.test;

/**
 * Enumeration of the testable methods.
 */
public interface TestedMethod {

    public interface Index extends TestedMethod {}
    public interface Save extends TestedMethod {}
    public interface SaveBulk extends TestedMethod {}
    public interface FindAll extends TestedMethod {}
    public interface FindAllById extends TestedMethod {}
    public interface FindAllPageable extends TestedMethod {}
    public interface FindAllSorted extends TestedMethod {}
    public interface FindOne extends TestedMethod {}
    public interface Exist extends TestedMethod {}
    public interface Count extends TestedMethod {}
    public interface DeleteAll extends TestedMethod {}
    public interface DeleteAllById extends TestedMethod {}
    public interface Delete extends TestedMethod {}
    public interface DeleteById extends TestedMethod {}
}
