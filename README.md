# rcproxy

##1.Introduction

  RCProxy, short for Redis Cluster Proxy, is a redis proxy built on redis 3.0(see [redis cluster spec](http://redis.io/topics/cluster-spec "悬停显示"). It aims to be simple, robust and efficient. It caches the cluster topology and implements the MOVED and ASK redirection. Cluster topology is updated on backend server failure or MOVED redirection received. Client is totally transpanrent about the backend data migration.
 
##2.Architecture
![](https://github.com/collinmsn/rcproxy/blob/master/rcproxy.png)

  Each client connection is wrapped with a session, which spawns two goroutines to read request from and write response to the client. Each session appends it's request to dispatcher's request queue, then dispatcher route request to the right task runner according key hash and slot table. Task runner sends requests to its backend server and read responses from it.
  Upon cluster topology changed, backend server will response MOVED or ASK error. These error is handled by session, by sending request to destination server directly. Session will trigger dispatcher to update slot info on MOVED error. When connection error is returned by task runner, session will trigger dispather to reload topology.

##3.Performance


 

