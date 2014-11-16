# What is data-fusion-server?

`data-fusion-server` is the backend implementation of the [DataFusion](https://github.com/cyme/DataFusion) engine.
It is a Node-js application that connects with the devices and offers data syncing services.

The project is in development.

# Features

#### Relational database

Client objects are organized in subclasses that follow a flexible schema.
Objects may refer to other objects through relational properties (relations).

#### Cloud data store

The server persists the clients objects in the cloud.

#### Data syncing

Clients participating to the same syncing group share the same data set.
The server ensures that the data is replicated across all clients
and that changes are propagated as necessary.

#### Conflict resolution

Conflicts may arise from concurrent changes to the same object by multiple clients.
Changes to distinct object properties are merged.
Changes to the same object property are transparently resolved using versions.

#### Change notifications

Clients gets notified of object changes initiated by others in the same syncing group.

#### Client-side caching & Offline

Clients maintain an object cache, which is used to speed up operations and allow clients to operate without connectivity.

#### Static & dynamic queries

Clients can issue object queries to the clients using a predicate.
Queries can be either static or dynamic.
Static queries return a static list of qualifying objects.
Dynamic queries return a live list of qualifying objects that is updated as objects qualify or disqualify for the query.

#### WebSocket & HTTP support

The server accepts WebSocket and HTTP connections with clients.
Clients normally implement WebSockets, for optimal performance.
HTTP support is provided for instrumentation and debugging purposes.

# Operations

## Summary of client operations

Clients establish a **WebSocket** connection with the server.
**HTTP** support is also provided for debugging.
Clients initialize a session with an **INIT** message.

A **syncing group** is the set of clients that share a data set.
Clients can join or leave a syncing group with a **JOIN** or **LEAVE** message.

Clients can issue **queries** on objects by specifying a subclass and a predicate.
A query predicate is a boolean expression that tests an object properties.
Join queries are not supported natively, as queries are limited to testing properties of
a single subclass.

Queries are either static or dynamic.
**Static queries** are issued with a **FETCH** message.
The server responds with a one-time list of objects that satisfy the predicate.
**Dynamic queries** are initiated with a **WATCH** message.
The server responds with an initial list of objects that satisfy the predicate.
The server then notifies the client of any subsequent changes to the qualifying set, i.e. objects that
qualify or disqualify as a result of updates, creations or deletions.
When a client is no longer interested in receiving to query updates, it issues an **UNWATCH** message.

The objects that a client has instantiated in memory as the result of queries constitute its **working set**.
All objects in the client working set are live, i.e. the client is notified by the server
of all changes made by other clients to those objects, such as object updates and object deletions.
The client informs the server that an object instance has been finalized with the **FORGET** message.

Clients notify the server of changes they make to the data set with a **SYNC** message.
Changes are reported as a combination of object updates, object creations and object deletions.

Clients are asynchronously notified by the server of query updates and of changes made by others to the data set.
**Change notifications** consist in:

- newly qualified objects for each outstanding dynamic query
- disqualifed objects for each outstanding dynamic query
- updates to live objects
- object creations, if qualifying for a query
- object deletions, if disqualifying for a query

Such notifications take the form of server-initiated WebSocket messages.
Notifications are acknowledged by the client.

Everytime the server provides a client with a list of objects to instantiate, e.g. query results
or server notifications, it also provides the **transitive closure** of those objects.
That is, the list of objects that can be reached directly and indirectly from those objects
when following the relational object graph.
The client is expected to instantiate the entire transitive closure.
This ensures that the entire object graph visible to a client is resident and can be
seamlessly navigated.

If no connectivity is available when the client starts up, the client operates in the **offline mode**.
While offline:

- static and dynamic query requests execute entirely out of the client cache
- other requests such as syncing group join or leave fail
- object changes are tracked

This allows the client to seamlessly operate in a degraded yet functional mode while offline

When connectivity is established, the client issues an **INIT** message and reports to
the server its full working set, i.e. the list of all objects instantiated from the client cache.
The server responds with refreshed object data if needed.

Once the session is initialized, the client reports its full state as follows:

- register all outstanding dynamic queries with a **WATCH** message
- report all outstanding object changes with a **SYNC** message

If connectivity is lost after the client has started and established an initial connection with the server,
the client switches to the offline mode.
Any pending request is aborted and restarted in the offline mode.

When connectivity is re-established, the client issues a **RECONNECT** message and
report its incremental working set, i.e. the list of all objects instantiated from the client cache
since the connection was lost.

If the reconnection is successful, the server responds with refreshed object data if needed and any missed change notification.
Once the client has successfully reconnected, it reports a differential state as follows:

- register all dynamic queries issued since the loss of connectivity and still outstanding with a **WATCH** message
- unregister all dynamic queries closed since the loss of connectivity with an **UNWATCH** message
- report all outstanding object changes with a **SYNC** message

However the reconnection fails and the server responds with an error if the client state is no longer available.
In this case the client has to re-initialize a session with an **INIT** message and follow the
full session initialization as already described.

Clients maintain an **object cache** in their local store.
The cache tracks the latest known state of the data set.
The following situations trigger a cache lookup:

- the calculation of the initial results of dynamic queries
- the execution of static and dynamic queries in the offline mode.


## Summary of server operations

The server persists the client objects in a **data store**.
The current implementation uses the Parse cloud data service as its store.
A migration to MongoDB is planned.
The server maintains a **cache** of active objects.

The server **pipelines** the processing of all client requests to maximize throughput.
In other terms, the server does not wait for a client request to complete before
handling the next queued request.
As consecutive requests may operate on the same data, it is necessary to
coordinate requests so that read-after-write, write-after-read and
write-after-write dependencies are properly handled.

For example, imagine client 1 issues a dynamic query searching for all objects
of subclass "customer" with property "city" set to "Portland".
Client 2 shortly after syncs an update for object A of subclass "customer", changing
its property "city" from "Portland" to "San Diego".
The server starts processing the first request and queries the data store for qualifying objects.
It then moves on to the second request while the data store processes the query.
Coherency issues may arise if the requests are not coordinated.
For example, the query may return object A in its initial results but fail to report the status of object A as disqualifying after the update.

The server coordinates client requests with **snapshots**.
Snapshots are lightweight objects that capture the state of the data set at a given time
using a "copy-on-write" approach.
The server creates a snapshot everytime it receives a client request that operates on the data set.
This allows the server to pipeline the processing of requests to access the data set in various states without
requiring additional synchronization.

When processing sync client requests, the server **resolves conflicts** and propagates
the changes to other clients as necessary.
Conflicts can potentially arise when clients independently update the same object properties.
When a client updates an object property, the server verifies that the client had the most
current version of the object property.
Changes to stale data are rejected and ignored.

When a client deletes an object, the server ensure that any **Dangling object references**,
i.e. object properties referring the deleted object, are automatically set to the
**null reference**.
The server generates synthetic object updates that are sent to clients as necessary alongside
the deletion notification.
Dangling references in inactive objects, i.e. objects that are not in the server cache at the
time of the deletion, are fixed lazily when those objects are loaded in the cache.


## WebSocket and REST API

#### INIT
#### JOIN
#### LEAVE
#### FETCH
#### WATCH
#### UNWATCH
#### FORGET
#### SYNC

# In Progress / To Do

- Offline mode

    A minimal implementation of INIT is provided.
    A full offline-capable implementation of INIT and RECONNECT remains to be developed.

- Working set management

    FORGET remains to be implemented.
    
- Syncing group

    JOIN and LEAVE remain to be implemented.
    
- Query predicate

    Queries currently accept no user-specified predicate.
    All objects of the query subclass qualify.
    A predicate engine remains to be implemented.
    
- Static queries

    FETCH remains to be implemented
    
- Full client caching

    The client cache is primarily used as an offline backup.
    The cache logic will be extended to speed up operations in online mode as well.
    
- Transactions

    Multi-object transaction support in SYNC remains to be implemented.
    
- Migration to MongoDB

    The data store will be migrated to MongoDB