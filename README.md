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
Clients can join and leave a **syncing group**.
All clients in the syncing group share the same data set.

Clients can issue **queries** on objects by specifying a subclass and a predicate.
A query predicate is a boolean expression that tests an object properties.
Join queries are not supported natively, as queries are limited to testing properties of
a single subclass.

Queries are either static or dynamic.
**Static queries** return a one-time list of objects that satisfy the predicate.
**Dynamic queries** return a live list of objects that satisfy the predicate, i.e. the client
is notified when objects qualify or disqualify for the query after the initial results have
been sent. This may happen when objects are updated, created or deleted.

Clients maintain an **object cache** on their local store.
Client retrieve from the server objects that are absent from the client cache or cached
but out-dated.

The objects that a client has instantiated in memory constitute its **working set**.
All objects in the client working set are live, i.e. the client is notified by the server
of all changes made by others to those objects, such as object updates and object deletions.

Everytime the server provides a client with a list of objects to instantiate, e.g. query results
or a query update, it also provides the **transitive closure** of those objects.
That is, the list of objects that can be reached directly and indirectly from those objects
when following the relational object graph.
The client is expected to instantiate the entire transitive closure.
This ensures that the entire object graph visible to a client is resident and can be
seamlessly navigated.

Clients notify the server of changes they make to the data set with a **sync** request.
Changes consist in a combination of object updates, object creations and object deletions.

## Summary of server architecture

The server persists the client objects in a **data store**.
The current implementation uses the Parse cloud data service as its store.
A migration to MongoDB is planned.
The server maintains a **cache** of active objects.

The server **pipelines** all client requests to maximize throughput.
In other terms, the server does not wait for a client request to complete before
handling the next request.
As consecutive requests may operate on the same data, it is necessary to
coordinate requests so that read-after-write, write-after-read and
write-after-write dependencies are properly handled.

For example, let's say client A issues a dynamic query searching for all objects
of subclass "customer" with property "city" set to "Portland".
Client B shortly after syncs an update for an object of subclass "customer", changing
its property "city" from "Portland" to "San Diego".
The correct behavior is for the query to return the object in its initial search results
to client A, and then to notify client A that the object stopped qualifying for the query.

The server achieves this coordination with **snapshots**.
Snapshots are lightweight objects that capture the state of the data set at a given time
using a "copy-on-write" approach.
Requests create each a snapshot of the data set and keep it throughout their execution.
This allows for concurrent requests to access the data set in various states without
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

## REST API

#### init
#### join
#### leave
#### search
#### watch
#### unwatch
#### retrieve
#### forget
#### sync

# To Do

- syncing groups
- query predicate
- working set management - retrieve and forget
- transactions
- migration to mongoDB
