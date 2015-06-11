package com.adform.storm.aerospike.trident.state;

import backtype.storm.task.IMetricsContext;
import com.adform.storm.aerospike.trident.state.util.AerospikeOptions;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import storm.trident.state.*;
import storm.trident.state.map.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * Persists a map state to an Aerospike store. The map state key
 * is used to determine the Aerospike record record key.
 *
 * Supports non-transactional, transactional and opaque states.
 *
 * In this implementation, each map value is stored in a bin.
 *
 */
@SuppressWarnings("unchecked")
public class AerospikeSingleBinMapState<T> extends AerospikeMapState<T> {

    private static final int BATCH_READ_LIMIT = 5000;

    private final String binName;
    private final KeyExtractor keyExtractor;

    private AerospikeSingleBinMapState(String binName, AerospikeOptions options, KeyExtractor keyExtractor) {
        super(options);
        this.binName = binName;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
       // Batch writes are not yet supported by Aerospike, so writes must be done via individual requests.
       // We could multithread write requests to achieve higher throughput. For now, write sequentially.
       for (int i = 0; i < keys.size(); i++) {

           ExtractedKey extractedKey = keyExtractor.build(keys.get(i));

           Key key;
           if (options.keyType == AerospikeOptions.AerospikeKeyType.LONG) {
               key = new Key(options.namespace, options.set, (Long) extractedKey.recordKey);
           } else {
               key = new Key(options.namespace, options.set, (String) extractedKey.recordKey);
           }

           Bin bin = new Bin(binName, serializerHelper.toSimpleType(vals.get(i)));
           client.put(null, key, bin);

       }
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {

        List<T> resultFromBatchGet = new ArrayList<>();

        for (int batch = 0; keys.size() > BATCH_READ_LIMIT * batch; batch++) {

            int numKeysToProcess = keys.size() - batch * BATCH_READ_LIMIT;
            if (numKeysToProcess > BATCH_READ_LIMIT) numKeysToProcess = BATCH_READ_LIMIT;

            Key[] aerospikeKeys = new Key[numKeysToProcess];
            int offset = batch * BATCH_READ_LIMIT;
            for (int i = 0; i < numKeysToProcess; i++) {
                ExtractedKey key = keyExtractor.build(keys.get(i + offset));
                if (options.keyType == AerospikeOptions.AerospikeKeyType.LONG) {
                    aerospikeKeys[i] = new Key(options.namespace, options.set, (Long) key.recordKey);
                } else {
                    aerospikeKeys[i] = new Key(options.namespace, options.set, (String) key.recordKey);
                }
            }

            Record[] records = client.get(null, aerospikeKeys, binName);

            for (Record record : records) {
                if (record != null) {
                    resultFromBatchGet.add((T) serializerHelper.toComplexType(record.getValue(binName)));
                } else {
                    resultFromBatchGet.add(null);
                }
            }

        }

        assert resultFromBatchGet.size() == keys.size();

        return resultFromBatchGet;

    }

    private static class Factory implements StateFactory {

        private String binName;
        private final AerospikeOptions options;
        private final StateType stateType;
        private final KeyExtractor keyExtractor;

        public Factory(String binName, AerospikeOptions options, StateType stateType, KeyExtractor keyExtractor) {
            this.binName = binName;
            this.options = options;
            this.stateType = stateType;
            this.keyExtractor = keyExtractor;
            if (options.serializerHelper == null) {
                options.serializerHelper = DEFAULT_SERIALIZERS.get(stateType);
                if (options.serializerHelper == null) {
                    throw new RuntimeException("Could not find serialization helper for state type: " + stateType);
                }
            }
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            AerospikeSingleBinMapState aerospikeState = new AerospikeSingleBinMapState(binName, options, keyExtractor);
            CachedMap cachedMap = new CachedMap(aerospikeState, 1000);
            MapState mapState = null;
            if (stateType == StateType.NON_TRANSACTIONAL) {
                mapState = NonTransactionalMap.build(cachedMap);
            } else if (stateType == StateType.OPAQUE) {
                mapState = OpaqueMap.build(cachedMap);
            } else if (stateType == StateType.TRANSACTIONAL) {
                mapState = TransactionalMap.build(cachedMap);
            }
            return mapState;
        }

    }

    public static interface KeyExtractor extends Serializable {
        public ExtractedKey build(List<Object> key);
    }

    public static class DefaultKeyExtractor implements KeyExtractor {

        @Override
        public ExtractedKey build(List<Object> key) {
            if (key.size() == 1) {
                return new ExtractedKey(key.get(0));
            } else {
                throw new RuntimeException("Unable to extract recordKey");
            }
        }

    }

    public static class ExtractedKey {

        public final Object recordKey;

        public ExtractedKey(Object aerospikeKey) {
            this.recordKey = aerospikeKey;
        }

    }

    public static StateFactory getNonTransactional(String binName, AerospikeOptions options) {
        return getNonTransactional(binName, options, new DefaultKeyExtractor());
    }

    public static StateFactory getNonTransactional(String binName, AerospikeOptions options, KeyExtractor keyExtractor) {
        return new Factory(binName, options, StateType.NON_TRANSACTIONAL, keyExtractor);
    }

    public static StateFactory getTransactional(String binName, AerospikeOptions options) {
        return getTransactional(binName, options, new DefaultKeyExtractor());
    }

    public static StateFactory getTransactional(String binName, AerospikeOptions options, KeyExtractor keyExtractor) {
        return new Factory(binName, options, StateType.TRANSACTIONAL, keyExtractor);
    }

    public static StateFactory getOpaque(String binName, AerospikeOptions options) {
        return getOpaque(binName, options, new DefaultKeyExtractor());
    }

    public static StateFactory getOpaque(String binName, AerospikeOptions options, KeyExtractor keyExtractor) {
        return new Factory(binName, options, StateType.OPAQUE, keyExtractor);
    }

}