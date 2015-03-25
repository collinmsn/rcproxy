# rcproxy

##1.Introduction

RCProxy, short for Redis Cluster Proxy, is a redis proxy built on redis 3.0(see [redis cluster spec](http://redis.io/topics/cluster-spec "悬停显示"). It aims to be simple, robust and efficient. It caches the cluster topology and implements the MOVED and ASK redirection. Cluster topology is updated on backend server failure or MOVED redirection received. Client is totally transpanrent about the backend data migration.
 
##2.Architecture


 

