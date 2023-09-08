# datastax-java-driver

The assignment was to implement custom request types for DataStax Cassandra Java Driver following Producer/Consumer pattern.

`CustomRequestProcessor` was implemented to return `Producer<Row>` result for CQL-compatible requests.
The solution
 - is thread-safe,
 - stores temporary items in a queue, and drains it whenever the consumer requests more items,
 - has a [non-blocking, lock-free queue-drain pattern using atomic variables](https://akarnokd.blogspot.com/2015/05/operator-concurrency-primitives_11.html),
 - uses `ForkJoinPool` for lock-free data pumping,
 - supports `Producer.cancel` to avoid data pumping when no longer required,
 - implements `filter`, `map` and `reduce` methods for producers that allows a data flow preserving the above-mentioned features.
 - covered with unit and integration tests.
