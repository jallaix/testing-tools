## Spring Data Elasticsearch Test

This module provides a way to test an Elasticsearch index through the Spring Data framework.
It basically integrates Tanguy Leroux's [Elasticsearch test](https://github.com/tlrx/elasticsearch-test) test unit framework with the [Spring Data Elasticsearch](https://github.com/spring-projects/spring-data-elasticsearch) framework.

You will be able to :
- Test all standard CRUD methods provided by the `ElasticsearchRepository` interface without writing a single test.
- Test all custom CRUD methods you define in your custom `ElasticsearchRepository` interface.
