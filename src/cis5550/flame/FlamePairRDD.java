package cis5550.flame;

import cis5550.kvs.KVSClient;
import java.util.List;
import java.io.Serializable;

public interface FlamePairRDD {
  public interface TwoStringsToString extends Serializable {
  	public String op(String a, String b);
  };

  public interface PairToPairIterable extends Serializable {
    Iterable<FlamePair> op(FlamePair a) throws Exception;
  };
  
  /**
   * Lambda interface with context for batched KVS operations in flatMapToPair.
   */
  public interface PairToPairIterableWithContext extends Serializable {
    Iterable<FlamePair> op(FlamePair pair, KVSClient kvsReader, BatchedKVSClient batchedWriter) throws Exception;
  };

  public interface PairToStringIterable extends Serializable {
    Iterable<String> op(FlamePair a) throws Exception;
  };
  
  /**
   * Lambda interface with context for batched KVS operations in flatMap.
   */
  public interface PairToStringIterableWithContext extends Serializable {
    Iterable<String> op(FlamePair pair, KVSClient kvsReader, BatchedKVSClient batchedWriter) throws Exception;
  };

  // collect() should return a list that contains all the elements in the PairRDD.

  public List<FlamePair> collect() throws Exception;

  // foldByKey() folds all the values that are associated with a given key in the
  // current PairRDD, and returns a new PairRDD with the resulting keys and values.
  // Formally, the new PairRDD should contain a pair (k,v) for each distinct key k 
  // in the current PairRDD, where v is computed as follows: Let v_1,...,v_N be the 
  // values associated with k in the current PairRDD (in other words, the current 
  // PairRDD contains (k,v_1),(k,v_2),...,(k,v_N)). Then the provided lambda should 
  // be invoked once for each v_i, with that v_i as the second argument. The first
  // invocation should use 'zeroElement' as its first argument, and each subsequent
  // invocation should use the result of the previous one. v is the result of the
  // last invocation.

	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception;
	
	// Parallel version of foldByKey with batched I/O for better performance on large datasets.
	// Uses BatchedKVSClient to reduce network overhead and improve throughput.
	public FlamePairRDD foldByKeyParallel(String zeroElement, TwoStringsToString lambda) throws Exception;

  // saveAsTable() should cause a table with the specified name to appear 
  // in the KVS that contains the data from this PairRDD. The table should 
  // have a row for each unique key in the PairRDD, and the different values
  // that are associated with this key should be in different columns. The
  // names of the columns can be anything.

  public void saveAsTable(String tableNameArg) throws Exception;
  
  // flatMap() should invoke the provided lambda once for each pair in the PairRDD, 
  // and it should return a new RDD that contains all the strings from the Iterables 
  // the lambda invocations have returned. It is okay for the same string to appear 
  // more than once in the output; in this case, the RDD should contain multiple 
  // copies of that string. The lambda is allowed to return null or an empty Iterable.

  public FlameRDD flatMap(PairToStringIterable lambda) throws Exception;
  
  // Parallel version of flatMap with batched I/O and context; uses bounded queues and
  // worker-grouped batching for better throughput on CPU-heavy lambdas.
  // Provides shared BatchedKVSClient for internal writes pooled across all threads.
  public FlameRDD flatMapParallel(PairToStringIterableWithContext lambda) throws Exception;

  // destroy() should delete the underlying table in the key-value store.
  // Any future invocations of any method on this RDD should throw an
  // exception.

  public void destroy() throws Exception;

  // flatMapToPair() is analogous to flatMap(), except that the lambda returns pairs 
  // instead of strings, and tha tthe output is a PairRDD instead of a normal RDD.

  public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception;
  
  // Parallel version of flatMapToPair with batched I/O and context; uses bounded queues and
  // worker-grouped batching for better throughput on CPU-heavy lambdas.
  // Provides shared BatchedKVSClient for internal writes pooled across all threads.
  public FlamePairRDD flatMapToPairParallel(PairToPairIterableWithContext lambda) throws Exception;

  // join() joins the current PairRDD A with another PairRDD B. Suppose A contains
  // a pair (k,v_A) and B contains a pair (k,v_B). Then the result should contain
  // a pair (k,v_A+","+v_B).

	public FlamePairRDD join(FlamePairRDD other) throws Exception;
	
  // Parallel version of join with batched I/O; uses ParallelLambdaExecutor and
  // BatchedKVSClient for better throughput on large datasets.
  public FlamePairRDD joinParallel(FlamePairRDD other) throws Exception;

  // This method should return a new PairRDD that contains, for each key k that exists 
  // in either the original RDD or in R, a pair (k,"[X],[Y]"), where X and Y are 
  // comma-separated lists of the values from the original RDD and from R, respectively. 
  // For instance, if the original RDD contains (fruit,apple) and (fruit,banana) and 
  // R contains (fruit,cherry), (fruit,date) and (fruit,fig), the result should contain 
  // a pair with key fruit and value [apple,banana],[cherry,date,fig]. This method is 
  // extra credit in HW7; if you do not implement it, please return 'null'.

  public FlamePairRDD cogroup(FlamePairRDD other) throws Exception;
  
  // Parallel version of cogroup with batched I/O and optimized key processing.
  // Processes keys in parallel batches instead of collecting all keys first, reducing memory
  // usage and improving performance on large datasets.
  public FlamePairRDD cogroupParallel(FlamePairRDD other) throws Exception;
}
