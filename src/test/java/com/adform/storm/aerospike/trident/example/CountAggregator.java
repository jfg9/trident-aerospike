package com.adform.storm.aerospike.trident.example;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class CountAggregator implements CombinerAggregator<Number> {

    @Override
    public Number init(TridentTuple tridentTuple) {
        return 1L;
    }

    @Override
    public Number combine(Number val1, Number val2) {
        return val1.longValue() + val2.longValue();
    }

    @Override
    public Number zero() {
        return 0L;
    }

}