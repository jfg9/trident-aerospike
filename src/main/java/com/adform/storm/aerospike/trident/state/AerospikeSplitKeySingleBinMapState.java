package com.adform.storm.aerospike.trident.state;

import backtype.storm.task.IMetricsContext;
import com.adform.storm.aerospike.trident.state.util.AerospikeOptions;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import storm.trident.state.*;
import storm.trident.state.map.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * Persists a map state to an Aerospike store. The map state key
 * is split to determine the Aerospike record record key and map key.
 *
 * Supports non-transactional, transactional and opaque states.
 *
 * This implementation uses the Aerospike map type to store a map of
 * counts in a bin. For handling maps, we requires use of user
 * defined functions on the server for retrieval and manipulation as
 * otherwise it is not possible to perform reads or writes on maps
 * without reading the whole map to the client.
 *
 */
@SuppressWarnings("unchecked")
public class AerospikeSplitKeySingleBinMapState<T> extends AerospikeMapState<T> {

    private static final String UDF_MODULE_NAME = "map_ops";
    private static final String UDF_READ_FUNCTION = "read_val";
    private static final String UDF_WRITE_FUNCTION = "write_val";

    private final String binName;
    private final KeyExtractor keyExtractor;

    private AerospikeSplitKeySingleBinMapState(String binName, AerospikeOptions options, KeyExtractor keyExtractor) {
        super(options);
        this.binName = binName;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {

       for (int i = 0; i < keys.size(); i++) {

           ExtractedKey extractedKey = keyExtractor.build(keys.get(i));

           Key key;
           if (options.keyType == AerospikeOptions.AerospikeKeyType.LONG) {
               key = new Key(options.namespace, options.set, (Long) extractedKey.recordKey);
           } else {
               key = new Key(options.namespace, options.set, (String) extractedKey.recordKey);
           }

           client.execute(null, key, UDF_MODULE_NAME, UDF_WRITE_FUNCTION,
                   Value.get(binName),
                   Value.get(extractedKey.mapKey),
                   serializerHelper.toSimpleType(vals.get(i)));

       }

    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {

        ExtractedKey[] extractedKeys = new ExtractedKey[keys.size()];
        List<T> resultsFromUDF = new ArrayList<>();

        for (int i = 0; i < keys.size(); i++) {

            extractedKeys[i] = keyExtractor.build(keys.get(i));

            Key key;
            if (options.keyType == AerospikeOptions.AerospikeKeyType.LONG) {
                key = new Key(options.namespace, options.set, (Long) extractedKeys[i].recordKey);
            } else {
                key = new Key(options.namespace, options.set, (String) extractedKeys[i].recordKey);
            }

            Object object = client.execute(null, key, UDF_MODULE_NAME, UDF_READ_FUNCTION,
                    Value.get(binName), Value.get(extractedKeys[i].mapKey));

            resultsFromUDF.add((T) serializerHelper.toComplexType(object));

        }

        assert resultsFromUDF.size() == keys.size();

        return resultsFromUDF;

    }

    private static class Factory implements StateFactory {

        private final String binName;
        private final AerospikeOptions options;
        private final StateType stateType;
        private final KeyExtractor keyAndBinFactory;

        public Factory(String binName, AerospikeOptions options, StateType stateType, KeyExtractor keyAndBinFactory) {
            this.binName = binName;
            this.options = options;
            this.stateType = stateType;
            this.keyAndBinFactory = keyAndBinFactory;
            if (options.serializerHelper == null) {
                options.serializerHelper = DEFAULT_SERIALIZERS.get(stateType);
                if (options.serializerHelper == null) {
                    throw new RuntimeException("Could not find serialization helper for state type: " + stateType);
                }
            }
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            AerospikeSplitKeySingleBinMapState aerospikeState = new AerospikeSplitKeySingleBinMapState(binName, options, keyAndBinFactory);
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
            if (key.size() == 2) {
                return new ExtractedKey(key.get(0), key.get(1));
            } else {
                throw new RuntimeException("Unable to extract recordKey - must have composite recordKey with 2 elements");
            }
        }

    }

    public static class ExtractedKey {

        public final Object recordKey;
        public final Object mapKey;

        public ExtractedKey(Object aerospikeKey, Object mapKey) {
            this.recordKey = aerospikeKey;
            this.mapKey = mapKey;
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