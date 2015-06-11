package com.adform.storm.aerospike.trident.state;

import com.adform.storm.aerospike.trident.state.util.AerospikeOptions;
import com.adform.storm.aerospike.trident.state.util.SerializationUtils.NonTransactionalSerializerHelper;
import com.adform.storm.aerospike.trident.state.util.SerializationUtils.OpaqueTransactionalSerializerHelper;
import com.adform.storm.aerospike.trident.state.util.SerializationUtils.SerializerHelper;
import com.adform.storm.aerospike.trident.state.util.SerializationUtils.TransactionalSerializerHelper;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import org.apache.storm.guava.collect.ImmutableMap;
import org.apache.storm.guava.collect.Maps;
import storm.trident.state.StateType;
import storm.trident.state.map.IBackingMap;
import java.util.EnumMap;

abstract class AerospikeMapState<T> implements IBackingMap<T> {

    protected static final EnumMap<StateType, SerializerHelper<?>> DEFAULT_SERIALIZERS = Maps.newEnumMap(ImmutableMap.of(
            StateType.NON_TRANSACTIONAL, new NonTransactionalSerializerHelper(),
            StateType.TRANSACTIONAL, new TransactionalSerializerHelper(),
            StateType.OPAQUE, new OpaqueTransactionalSerializerHelper()
    ));

    private static synchronized void configureClient(AerospikeOptions options) {
        if (client == null) {
            ClientPolicy policy = new ClientPolicy();
            policy.writePolicyDefault.maxRetries = 10;
            policy.writePolicyDefault.recordExistsAction = RecordExistsAction.UPDATE;
            policy.readPolicyDefault.timeout = options.readTimeout;
            policy.writePolicyDefault.timeout = options.writeTimeout;
            policy.maxThreads = options.maxThreads;
            client = new AerospikeClient(policy, options.host, options.port);
        }
    }

    protected static transient volatile AerospikeClient client = null;

    protected final AerospikeOptions options;
    protected final SerializerHelper serializerHelper;

    public AerospikeMapState(AerospikeOptions options) {
        configureClient(options);
        this.options = options;
        this.serializerHelper = options.serializerHelper;
    }

}