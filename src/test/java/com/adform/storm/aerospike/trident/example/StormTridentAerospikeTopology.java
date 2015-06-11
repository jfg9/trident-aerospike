package com.adform.storm.aerospike.trident.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.adform.storm.aerospike.trident.state.AerospikeSingleBinMapState;
import com.adform.storm.aerospike.trident.state.AerospikeSplitKeySingleBinMapState;
import com.adform.storm.aerospike.trident.state.util.AerospikeOptions;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;

public class StormTridentAerospikeTopology {

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(4);
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCounter", conf, buildTopology1(drpc));
        cluster.submitTopology("fruitCounter", conf, buildTopology2(drpc));
        for (int i = 0; i < 100; i++) {
            System.out.println("DRPC RESULT 1: " + drpc.execute("words", "cat the dog jumped"));
            System.out.println("DRPC RESULT 2: " + drpc.execute("fruits", "2 orange"));
            Thread.sleep(1000);
        }
    }

    /* Example topology storing a single count for each Aerospike key */
    private static StormTopology buildTopology1(LocalDRPC drpc) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 100,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("to be or not to be the person"));
        spout.setCycle(true);

        AerospikeOptions options = new AerospikeOptions();
        options.set = "words";
        options.keyType = AerospikeOptions.AerospikeKeyType.STRING;

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts =
                topology.newStream("spout1", spout)
                        .parallelismHint(4)
                        .each(new Fields("sentence"), new Split(), new Fields("word"))
                        .groupBy(new Fields("word"))
                        .persistentAggregate(AerospikeSingleBinMapState.getTransactional("count", options),
                                new CountAggregator(), new Fields("count"))
                        .parallelismHint(4);

        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

        return topology.build();
    }

    /* Example topology storing a map of counts for each Aerospike key */
    private static StormTopology buildTopology2(LocalDRPC drpc) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("customerid", "fruit"), 100,
                new Values(1L, "apple"),
                new Values(1L, "apple"),
                new Values(1L, "apple"),
                new Values(1L, "pear"),
                new Values(2L, "pear"),
                new Values(2L, "orange"),
                new Values(3L, "orange"),
                new Values(3L, "orange"),
                new Values(3L, "banana"));
        spout.setCycle(true);

        AerospikeOptions options = new AerospikeOptions();
        options.set = "customers";
        options.keyType = AerospikeOptions.AerospikeKeyType.LONG;

        TridentTopology topology = new TridentTopology();
        TridentState fruitCounts =
                topology.newStream("spout2", spout)
                        .parallelismHint(4)
                        .groupBy(new Fields("customerid", "fruit"))
                        .persistentAggregate(AerospikeSplitKeySingleBinMapState.getTransactional("fruits", options),
                                new CountAggregator(), new Fields("count"))
                        .parallelismHint(4);

        topology.newDRPCStream("fruits", drpc)
                .each(new Fields("args"), new SplitIntoTwo(), new Fields("customerid", "fruit"))
                .groupBy(new Fields("customerid", "fruit"))
                .stateQuery(fruitCounts, new Fields("customerid", "fruit"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .project(new Fields("count"));

        return topology.build();
    }

    public static class SplitIntoTwo extends BaseFunction {

        public void execute(TridentTuple tuple, TridentCollector collector) {
            String[] arr = tuple.getString(0).split(" ");
            if (arr.length == 2) {
                collector.emit(new Values(Long.valueOf(arr[0].trim()), arr[1].trim()));
            }
        }

    }

}