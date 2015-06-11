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
 * is split to determine the Aerospike record key and bin.
 *
 * Supports non-transactional, transactional and opaque states.
 *
 * In this implementation, Aerospike bins correspond to
 * map keys and bin values correspond to map values.
 * This does not use the Aerospike map type and does not
 * require user defined functions to update state.
 *
 */
@SuppressWarnings("unchecked")
public class AerospikeSplitKeyMultiBinMapState<T> extends AerospikeMapState<T> {

    private static final int BATCH_READ_LIMIT = 5000;

    private KeyAndBinExtractor keyAndBinExtractor;

    private AerospikeSplitKeyMultiBinMapState(AerospikeOptions options, KeyAndBinExtractor keyAndBinExtractor) {
        super(options);
        this.keyAndBinExtractor = keyAndBinExtractor;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        // Batch writes are not yet supported by Aerospike, so writes must be done via individual requests.
        // We could multithread write requests to achieve higher throughput. For now, write sequentially.
        for (int i = 0; i < keys.size(); i++) {

            KeyAndBin keyAndBin = keyAndBinExtractor.build(keys.get(i));

            Key key;
            if (options.keyType == AerospikeOptions.AerospikeKeyType.LONG) {
                key = new Key(options.namespace, options.set, (Long) keyAndBin.recordKey);
            } else {
                key = new Key(options.namespace, options.set, (String) keyAndBin.recordKey);
            }

            Bin bin = new Bin(keyAndBin.bin, serializerHelper.toSimpleType(vals.get(i)));
            client.put(null, key, bin);

        }
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {

        List<T> result = new ArrayList<>();

        for (int batch = 0; keys.size() > BATCH_READ_LIMIT * batch; batch++) {

            int numKeysToProcess = keys.size() - batch * BATCH_READ_LIMIT;
            if (numKeysToProcess > BATCH_READ_LIMIT) numKeysToProcess = BATCH_READ_LIMIT;

            Key[] aerospikeKeys = new Key[numKeysToProcess];
            String[] binNames = new String[numKeysToProcess];
            int offset = batch * BATCH_READ_LIMIT;
            for (int i = 0; i < numKeysToProcess; i++) {
                KeyAndBin keyAndBin = keyAndBinExtractor.build(keys.get(i + offset));
                if (options.keyType == AerospikeOptions.AerospikeKeyType.LONG) {
                    aerospikeKeys[i] = new Key(options.namespace, options.set, (Long) keyAndBin.recordKey);
                } else {
                    aerospikeKeys[i] = new Key(options.namespace, options.set, (String) keyAndBin.recordKey);
                }
                binNames[i] = keyAndBin.bin;
            }

            Record[] records = client.get(null, aerospikeKeys);

            for (int i = 0; i > records.length; i++) {
                Record record = records[i];
                if (record != null) {
                    result.add((T) serializerHelper.toComplexType(record.getValue(binNames[i])));
                } else {
                    result.add(null);
                }
            }

        }

        assert result.size() == keys.size();

        return result;

    }

    private static class Factory implements StateFactory {

        private final AerospikeOptions options;
        private final StateType stateType;
        private final KeyAndBinExtractor keyAndBinExtractor;

        public Factory(AerospikeOptions options, StateType stateType, KeyAndBinExtractor keyAndBinExtractor) {
            this.options = options;
            this.stateType = stateType;
            this.keyAndBinExtractor = keyAndBinExtractor;
            if (options.serializerHelper == null) {
                options.serializerHelper = DEFAULT_SERIALIZERS.get(stateType);
                if (options.serializerHelper == null) {
                    throw new RuntimeException("Could not find serialization helper for state type: " + stateType);
                }
            }
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            AerospikeSplitKeyMultiBinMapState aerospikeState = new AerospikeSplitKeyMultiBinMapState(options, keyAndBinExtractor);
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

    public static interface KeyAndBinExtractor extends Serializable {
        public KeyAndBin build(List<Object> key);
    }

    public static class DefaultKeyAndBinExtractor implements KeyAndBinExtractor {

        @Override
        public KeyAndBin build(List<Object> key) {
            if (key.size() == 2) {
                return new KeyAndBin(key.get(0), (String) key.get(1));
            } else {
                throw new RuntimeException("Unable to extract recordKey and bin - must have composite recordKey with 2 elements");
            }
        }

    }

    public static class KeyAndBin {

        public final Object recordKey;
        public final String bin;

        public KeyAndBin(Object key, String bin) {
            this.recordKey = key;
            this.bin = bin;
        }

    }

    public static StateFactory getNonTransactional(AerospikeOptions options) {
        return getNonTransactional(options, new DefaultKeyAndBinExtractor());
    }

    public static StateFactory getNonTransactional(AerospikeOptions options, KeyAndBinExtractor keyExtractor) {
        return new Factory(options, StateType.NON_TRANSACTIONAL, keyExtractor);
    }

    public static StateFactory getTransactional(AerospikeOptions options) {
        return getTransactional(options, new DefaultKeyAndBinExtractor());
    }

    public static StateFactory getTransactional(AerospikeOptions options, KeyAndBinExtractor keyExtractor) {
        return new Factory(options, StateType.TRANSACTIONAL, keyExtractor);
    }

    public static StateFactory getOpaque(AerospikeOptions options) {
        return getOpaque(options, new DefaultKeyAndBinExtractor());
    }

    public static StateFactory getOpaque(AerospikeOptions options, KeyAndBinExtractor keyExtractor) {
        return new Factory(options, StateType.OPAQUE, keyExtractor);
    }

}