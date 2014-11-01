var express = require('express');
var bodyParser = require('body-parser');
var http = require('http');

// use ES5 and the es6-shim library from Paul Miller until node catches up to a recent enough version
// of V8 (3.27.x or newer) that better supports the ES6 collections

require('es6-shim');

var Condition = require('sync-utils').Condition;

var UserError = require('./user_error');
var Group = require('./group');
var Client = require('./client');
var Request = require('./request');
var Snapshot = require('./snapshot');
var Query = require('./query');
var Sync = require('./sync');

var app = express();
var server = http.Server(app);

/*
var io = require('socket.io')(server);
*/

app.use(bodyParser.json());

app.get('/init', function(req, res) {
    Request.handleHTTPRequest(req, res, init);
});

app.get('/watch', function(req, res) {
    Request.handleHTTPRequest(req, res, watch);
});

app.get('/unwatch', function(req, res) {
    Request.handleHTTPRequest(req, res, unwatch);
});

app.get('/forget', function(req, res) {
    Request.handleHTTPRequest(req, res, forget);
});

app.post('/sync', function(req, res) {
    Request.handleHTTPRequest(req, res, sync);
});

/*
io.on('connection', function (socket) {
    var     connection;
    
    connection = new Connection("websocket", socket);
    
    socket.on('init', function(data, responder) {
        var     request;
        request = new Request(connection, data, responder);
        init(request);
    });
    
    socket.on('watch', function(data, responder) {
        var     request;
        request = new Request(connection, data, responder);
        watch(request);
    });
    
    socket.on('unwatch', function(data, responder) {
        var     request;
        request = new Request(connection, data, responder);
        unwatch(request);
    });

    socket.on('forget', function(data, responder) {
        var     request;
        request = new Request(connection, data, responder);
        forget(request);
    });

    socket.on('sync', function(data, responder) {
        var     request;
        request = new Request(connection, data, responder);
        sync(request);
    });
});
*/

server.setTimeout(0);
server.listen(process.env.PORT, process.env.IP);
console.log("Listening on %s : %d", process.env.IP, process.env.PORT);

/*
TO DO:
- Implement client-allocated object IDs
    combination of client session token and unique client object id
    allows to make object creation asynchronous (no need to wait for Parse object id)
    but forces to maintain mapping table, a hit on performance and memory
*/


// init
// in:
// {
//      "session": "{session token}"
// }
// out:
// {
// }

function init(request)
{
    var     client;
    var     token;

console.log('init');
    token = request.getToken();
    client = Client.clients.get(token);
    if (client === undefined) {
        client = new Client(Group.group, token);
        client.setConnection(request.connection);
    }
console.log('done');
    request.respond();
console.log('client refcount: '+client.refcount);
    return;
}


// watch
// in:
// {
//      "session": "{session token}",
//      "subclass": "{class name}"
// }
// out:
// {
//      "query": {
//          "id": "{query id}"
//      },
//      "qualified": [
//          {
//              "subclass": "{class name}",
//              "id": "{object id}",
//              "version": #{version}
//          }
//      ],
//      "fetch": [
//          {
//              "subclass": "{class name}",
//              "id": "{object id}",
//              "version": #{version}
//          }
//      ]
//  }

function watch(request)
{
    var     client;
    var     data;
    var     subclass;
    var     query;
    var     snapshot;
    var     doneWithResults;
    var     readyToUpdate;
    var     currentPushCompleted;
    var     lastPushCompleted;
    var     qualified;
    var     qualifiedList;
    var     fetchList;

console.log('watch');

    client = request.connection.client;
    if (client === undefined) {
        request.error(new UserError("watch: session hasn't been initialized yet"));
        return;
    }

    // find a matching query, or create a new one if none exists

    data = request.getData();
    subclass = data.subclass;
    query = Query.getQueryForSubclass(client.group, subclass);

    // retain the query to ensure it sticks around until we can add the client to it

    query.retain();

    // create a snapshot of the data set and retain it

    snapshot = new Snapshot(client.group).retain();
    
    // capture the ready-to-update query condition and insert ourselves in the
    // query processing queue
    
    doneWithResults = new Condition();
    readyToUpdate = query.readyToUpdate;
    query.readyToUpdate = doneWithResults;

    // capture the last-push-completed client condition and insert ourselves in the
    // client push queue
    
    currentPushCompleted = new Condition();
    lastPushCompleted = client.lastPushCompleted;
    client.lastPushCompleted = currentPushCompleted;

    // wait for our turn to process the query

    readyToUpdate.wait()
    
    // then issue the query
    
    .then(function() {

        // add the client to the query

        query.addClient(client);
    
        // release the query to balance out the earlier retain

        query.release();
        
        // find the qualifying objects
        
        return query.find(client, snapshot);
    })
    
    // wait until we have found all qualifying objects
    // note that query result is guaranteed be sent to the client before any
    // update because of the ready-to-update condition.

    .then(function() {

        // make a shallow copy of the qualified set
        // we do this so that we can safely work with our list while the query may update
        // the qualified set

        qualified = new Set(query.qualified);
        
        // add the qualifying object to the client working set if it wasn't already there

        qualified.forEach(function(object) {
            
            // retain the object as we made a copy of the qualified list
            
            object.retain();
            
            client.addObjectToWorkingSet(object);
        });

        // we're done processing the query
        // unblock the next client in line to process it
    
        doneWithResults.signal();
        
        // identify and load the related objects that need to be fetched by the client
        
        return Query.getFetchList(client, qualified, snapshot);
    })
    
    // wait for all fetch objects to be loaded
    
    .then(function(fetch) {

        // add the fetch objects to the client working set

        fetch.forEach(function(object) {

            client.addObjectToWorkingSet(object);
            
            // release the object to keep the retain count in balance
            // (object was retained in getQueryFetchList and again in addObjectToWorkSet)
            
            object.release();
        });

        // create a formatted list of qualified and fetch objects for the client
        
        qualifiedList = Request.composeObjectList(qualified);
        fetchList = Request.composeObjectList(fetch);

        // we're now have a response formed
        // let's wait for our turn to respond to the client
        // this ensures proper ordering of responses and pushed messages

        return lastPushCompleted.wait();
    })
    
    .then(function() {
        
        // we can now send the response

        request.respond({
            query: { id: query.id },
            qualified: qualifiedList,
            fetch: fetchList
        });
        
        // signal we're done pushing the response
        
        currentPushCompleted.signal();
        
        // we're done. clean up
        
        qualified.forEach(function(object) {
            object.release();
        });
        snapshot.release();
    });
}

    
// unwatch
// in:
// {
//      "session": "{session token}",
//      "id": "{query id}"
// }
// out:
// {
// }

function unwatch(request)
{
    var     client;
    var     data;
    var     query;

console.log('unwatch');

    client = request.connection.client;
    if (client === undefined) {
        request.error(new UserError("unwatch: session hasn't been initialized yet"));
        return;
    }

    // find the query

    data = request.getData();
    query = client.group.queriesByID.get(data.id);
    if (query === undefined) {
        request.error(new UserError("unwatch: no such query"));
        return;
        
    }
    
    // ensure unwatche hasn't been called on this query yet
    
    if (query.onUnregister !== null) {
        request.error(new UserError("unwatch: query is already being unwatched"));
        return;
    }

    // install a callback to be called when the query reference count
    // reaches 0. In that callback we respond to the client. This
    // guarantees that the client does not get a response form unwatch
    // until all query-related messages have been sent.
    
    query.onUnregister = function() {
        
        request.respond({});
    };
    
    // now we can remove the client from the query. this will release the query object.
    
    query.removeClient(client);
    
    // we're done
}

// forget
// req: {
//      "session": "{session token}",
//      "sequence": #sequence,
//      "forget": [
//          {
//              "subclass": "{class name}",
//              "id": "{object id}"
//          }
//          ...
//      ]
// }
//
// res: {
//      "result": "ok" | "abort"
// }

function forget(request)
{
    var     client;
    var     data;
    var     snapshot;

console.log('forget');

    client = request.connection.client;
    if (client === undefined) {
        request.error(new UserError("forget: session hasn't been initialized yet"));
        return;
    }

    // create a snapshot and retain it
    
    snapshot = new Snapshot(client.group).retain();
    
    // XXX check here for integrity
    
    snapshot.release();
}

// sync
// req: {
//      "session": "session token",
//      "creations": [
//          {
//              "subclass": "{class name}",
//              "id": "{local id}",
//              "values": [
//                  [ "{property name}", "{string value}" | #{number value} ]
//                  [
//                      "{relation name}",
//                      {
//                          "type": "global",
//                          "subclass": "{class name}",
//                          "id": "{object id}"
//                      }
//                  ]
//                  [
//                      "{relation name}",
//                      {
//                          "type": "local",
//                          "subclass": "{class name}",
//                          "id": "{local id}"
//                      }
//                  ]
//                  ...
//              ]
//          }
//      ],
//      "deletions": [
//          {
//              "subclass": "{class name}",
//              "id": "{object id}",
//          }
//          ...
//      ],
//      "updates": [
//          {
//              "subclass": "{class name}",
//              "id": "{object id}",
//              "version": #{object base version},
//              "values": [
//                  [ "{property name}", "{string value}" | #{number value} ]
//                  [
//                      "{relation name}",
//                      {
//                          "type": "global",
//                          "subclass": "{class name}",
//                          "id": "{object id}"
//                      }
//                  ]
//                  [
//                      "{relation name}",
//                      {
//                          "type": "local",
//                          "subclass": "{class name}",
//                          "id": "{local id}"
//                      }
//                  ]
//                  ...
//              ]
//          }
//      ],
//      "transactions": [
//          {
//              "captured": [
//                  {
//                      "id": "object id of captured object 1",
//                      "keys": [
//                          "captured object property name"
//                      ]
//                  }
//              ],
//              "modified": [
//                  {
//                      "id": "object id of transaction modified object 1",
//                      "data": {
//                          "property name": [ "property value", "timestamp" ]
//                          ...
//                      }
//                  }
//              ],
//              "dependencies": [
//                  index of depending transaction
//                  ...
//              ]
//          }
//      ]
// }

// res: {
//      "ids": [
//          {
//              "subclass": "{class name}",
//              "id": "{object id}",
//              "local": "{local id}"
//          }
//          ...
//      ],
//      "voided": [
//          #{index of voided transaction}
//          ...
//      ]
// }
//
//  /* pushed (asynchronous data) */
//
//      "creation": {
//          "subclass": "{class name}",
//          "id": "{object id}",
//          "values": [
//              [ "{property name}", "{string value}" | #{number value} ]
//              [ "{relation name}",
//                {
//                  "type": "global",
//                  "subclass": "{class name}",
//                  "id": "{object id}"
//                } ]
//              ...
//          ],
//          "qualifying": [
//              "{query id}"
//              ...
//          ],
//          "fetch": [
//              {
//                  "subclass": "{class name}",
//                  "id": "{object id}",
//                  "version": #{version}
//              }
//              ...
//          ]
//      }
//
//      "deletion": {
//          "subclass": "{class name}",
//          "id": "{object id}",
//          "disqualifying": [
//              "{query id}
//              ...
//          ]
//      }
//
//      "update": {
//          "subclass": "{class name}",
//          "id": "{object id}",
//          "version" : #{version},
//          "sequence": #{sequence},
//          "values": [
//              [ "{property name}", "{string value}" | #{number value} ]
//              [
//                  "{relation name}",
//                  {
//                      "type": "global",
//                      "subclass": "{class name}",
//                      "id": "{object id}"
//                  }
//              ]
//              ...
//          ],
//          "qualifying": [
//              "{query id}"
//              ...
//          ],
//          "disqualifying": [
//              "{query id}"
//              ...
//          ],
//          "fetch": [
//              {
//                  "subclass": "{class name}",
//                  "id": "{object id}",
//                  "version": #{version}
//              }
//              ...
//          ]
//      }

function sync(request)
{
    var     data;
    var     client;
    var     creations;
    var     updates;
    var     deletions;
    var     creationsPromise;
    var     deletionsPromise;
    var     updatesPromise;
    var     creationsCommittedPromise;
    var     deletionsCommittedPromise;
    var     updatesCommittedPromise;
    var     clients;
    var     queries;
    var     clientsQueries;
    var     queriesClients;
    var     pushedPromise;
    var     processingState;

    client = request.connection.client;
    if (client === undefined) {
        request.error(new UserError("sync: session hasn't been initialized yet"));
        return;
    }
    
    console.log("sync starts for token %s", client.token);

    // extract the list of creations, deletions and updates from the request body
    
    data = request.getData();
    creationsPromise = Request.parseCreations(client, data);
    updatesPromise = Request.parseUpdates(client, data);
    deletionsPromise = Request.parseDeletions(client, data);

    Promise.settle([creationsPromise, deletionsPromise, updatesPromise])
 
    // process the client events and capture the processing state
    // and push to other clients the appropriate changes

    .then(function(results) {

        creations = results[0];
        deletions = results[1];
        updates = results[2];

        processingState =  Sync.prepareForProcessing(client, creations, deletions, updates);
        
        creationsCommittedPromise = processingState.creationsCommittedPromise;
        deletionsCommittedPromise = processingState.deletionsCommittedPromise;
        updatesCommittedPromise = processingState.updatesCommittedPromise;
        clients = processingState.clients;
        queries = processingState.queries;
        clientsQueries = processingState.clientsQueries;
        queriesClients = processingState.queriesClients;

        pushedPromise = Sync.processQueries(client, processingState);

        return creationsCommittedPromise;
    })
    
    // wait for the creations to be committed to the store
    // then return the new object ids to the client
    
    .then(function() {

        var     data;

        data = {};
        data.ids = [];
        creations.forEach(function(creation) {
            var     reference;
            
            reference = creation.object.reference;
            data.ids.push({
                subclass: reference.subclass,
                id: reference.globalID,
                local: reference.localID
            });
        });
console.log("sync succeeded");
        request.respond(data);
        
        return Promise.settle([deletionsCommittedPromise,
                                updatesCommittedPromise,
                                pushedPromise]);
    })

    // wait for all operations to commit to the store and for the push operations
    // to complete
    
    .then(function() {

        // now we can safely clean up

        Sync.cleanupAfterProcessing(processingState);
console.log('client refcount: '+client.refcount);
    })
    
    // if we got any error along the way, deal with it here
    
    .catch(function(error) {
        
        // if we got an error extracting the client request.
        // return an error from sync
        
        console.error("sync failed: ", error);
        
        // did we fail in Promise.settle?
        
        if (clientCreations === undefined) {
            
            // extract the values that may have resolved from the error
            
            clientCreations = error.resolved[0];
            clientDeletions = error.resolved[1];
            clientUpdates = error.resolved[2];
            
            // let the error be the first error we find
            
            error = (error.rejected[0] !== undefined ? error.rejected[0] :
                error.rejected[1] !== undefined ? error.rejected[1] :
                error.rejected[2]);
        }
        
        // clean up
        
        if (clientCreations !== undefined)
            clientCreations.forEach(function(creation) {
                creation.drop();
            });
        if (clientDeletions !== undefined)
            clientDeletions.forEach(function(deletion) {
                deletion.drop();
            });
        if (clientUpdates !== undefined)
            clientUpdates.forEach(function(update) {
                update.drop();
            });
        request.error(error);
    });
}
