package info.jallaix.spring.data.es.test;

/**
 * Enumeration of the testable methods.
 */
public interface DaoTestedMethod {

    public interface Index extends DaoTestedMethod {}
    public interface Save extends DaoTestedMethod {}
    public interface SaveBulk extends DaoTestedMethod {}
    public interface FindAll extends DaoTestedMethod {}
    public interface FindAllById extends DaoTestedMethod {}
    public interface FindAllPageable extends DaoTestedMethod {}
    public interface FindAllSorted extends DaoTestedMethod {}
    public interface FindOne extends DaoTestedMethod {}
    public interface Exist extends DaoTestedMethod {}
    public interface Count extends DaoTestedMethod {}
    public interface DeleteAll extends DaoTestedMethod {}
    public interface DeleteAllById extends DaoTestedMethod {}
    public interface Delete extends DaoTestedMethod {}
    public interface DeleteById extends DaoTestedMethod {}
}
