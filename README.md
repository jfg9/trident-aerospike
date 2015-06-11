# trident-aerospike
Storm Trident connectors for Aerospike, consisting of various implementations of an Aerospike Trident state.

This project currently provides three state implementations:
  - **AerospikeSingleBinMapState** - The map state key is used to determine the Aerospike record record key. Each map value is stored in a bin.
  - **AerospikeSplitKeyMultiBinMapState** - The map state key is split to determine the Aerospike record record key and bin. In this implementation, Aerospike bins correspond to map keys and bin values correspond to map values. This does not use the Aerospike map type and does not require user defined functions to update state.
  - **AerospikeSplitKeySingleBinMapState** - The map state key is split to determine the Aerospike record record key and map key. This implementation uses the Aerospike map type to store a map of counts in a bin. For handling maps, we require use of user defined functions on the server for retrieval and manipulation as otherwise it is not possible to perform reads or writes on maps without reading the whole map to the client.

All implementations support storing either non-transactional, transactional or opaque state, as described [here](https://storm.apache.org/documentation/Trident-state).

An example topology is provided for both the single-key and split-key map states.

Note that to use the AerospikeSplitKeySingleBinMapState, you must register the provided UDFs on your Aerospike instance:

```
aql> REGISTER MODULE 'map_ops.lua'
```
