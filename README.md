sharding-lib
============

General purpose sharding library implementation.

NodeRouter interface supports dinamically add/remove nodes.
Automatic rebalancing action is supported with Listener, when node list is changed.
To spread elements between nodes it use consistent hashing algorithm,
so it is guaranteed minimization of remapping elements after the node list is changed.
Supported backends are memcached (fastest) and cassandra (2.x is required).

