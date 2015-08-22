sharding-lib
============

General purpose sharding library implementation.

NodeRouter interface supports dinamically add/remove nodes. Implementation based on jgroup library.
Automatic rebalancing action is supported with RebalancingStrategy. Every time nodes list are changed, the nodesChanged() method is ivoked.
To spread elements between nodes it use consistent hashing algorithm, so it's guaranteed minimization of remapping elements after the node list is changed.

