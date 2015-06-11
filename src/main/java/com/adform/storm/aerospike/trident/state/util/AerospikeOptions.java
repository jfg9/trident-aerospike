package com.adform.storm.aerospike.trident.state.util;

import java.io.Serializable;

public class AerospikeOptions implements Serializable {

    public String host = "127.0.0.1";
    public int port = 3000;
    public String namespace = "test";
    public String set = "set";
    public AerospikeKeyType keyType = AerospikeKeyType.LONG;
    public int readTimeout = 1000;
    public int writeTimeout = 1000;
    public int maxThreads = 300;
    public SerializationUtils.SerializerHelper serializerHelper = null;

    public static enum AerospikeKeyType {
        LONG, STRING
    }

}