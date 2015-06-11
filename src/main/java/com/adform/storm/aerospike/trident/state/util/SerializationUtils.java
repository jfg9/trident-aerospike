package com.adform.storm.aerospike.trident.state.util;

import com.aerospike.client.Value;
import storm.trident.state.OpaqueValue;
import storm.trident.state.TransactionalValue;

import java.io.Serializable;
import java.util.List;

public class SerializationUtils {

    private SerializationUtils() {}

    public interface SerializerHelper<T> extends Serializable {

        Value toSimpleType(T value);
        T toComplexType(Object object);

    }

    public static class NonTransactionalSerializerHelper implements SerializerHelper<Object> {

        @Override
        public Value toSimpleType(Object value) {
            return Value.get(value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object toComplexType(Object value) {
            return value;
        }

    }

    public static class TransactionalSerializerHelper implements SerializerHelper<TransactionalValue> {

        @Override
        public Value toSimpleType(TransactionalValue value) {
            Value[] result = new Value[2];
            result[0] = Value.get(value.getVal());
            result[1] = Value.get(value.getTxid());
            return Value.get(result);
        }

        @Override
        @SuppressWarnings("unchecked")
        public TransactionalValue toComplexType(Object object) {
            if (object != null) {
                List<Object> values = (List<Object>) object;
                return new TransactionalValue((long) values.get(1), values.get(0));
            }
            return null;
        }

    }

    public static class OpaqueTransactionalSerializerHelper implements SerializerHelper<OpaqueValue> {

        @Override
        public Value toSimpleType(OpaqueValue value) {
            Value[] result = new Value[3];
            result[0] = Value.get(value.getCurr());
            result[1] = Value.get(value.getPrev());
            result[2] = Value.get(value.getCurrTxid());
            return Value.get(result);
        }

        @Override
        @SuppressWarnings("unchecked")
        public OpaqueValue toComplexType(Object object) {
            if (object != null) {
                List<Object> values = (List<Object>) object;
                return new OpaqueValue((long) values.get(2), values.get(0), values.get(1));
            }
            return null;
        }

    }

}