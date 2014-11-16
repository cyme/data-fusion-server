module.exports = Sync;

// use ES5 and the es6-shim library from Paul Miller until node catches up to a recent enough version
// of V8 (3.27.x or newer) that better supports the ES6 collections

require('es6-shim');

var Condition = require('sync-utils').Condition;
var Timer = require('sync-utils').Timer;

var ServerError = require('./server_error');
var UserError = require('./user_error');
var Request = require('./request');
var Reference = require('./reference');
var SyncedObject = require('./synced_object');
var Snapshot = require('./snapshot');
var Update = require('./update');
var Creation = require('./creation');
var Deletion = require('./deletion');



function Sync()
{
    
}

Sync.prepareForProcessing = function(client, creations, deletions, updates)
{
    var     snapshot;
    var     sequence;
    var     creationsCommittedPromise;
    var     deletionsCommittedPromise;
    var     updatesCommittedPromise;
    var     clients;
    var     queries;
    var     clientsQueries;
    var     queriesClients;
    var     lastPushesCompleted;
    var     currentPushesCompleted;
    var     queriesReadyToUpdate;
    var     queriesDoneWithResults;
    var     queryPromises;
    var     notifiedClients;
    var     relations;

    // create a snapshot of the data set and retain it
    
    snapshot = new Snapshot(client.group).retain();
    sequence = snapshot.sequence;

    // take a snapshot of:
    // - all clients
    // - all queries
    // - which queries are subscribed by each client
    // - which clients subscribe to each query
    // This insulate the asynchronous query processing from state
    // changes - e.g. a client subscribing to a new query.
    // The clients and queries captured in the snapshot are retained.

    clients = new Set();
    client.group.clients.forEach(function(client) {
        clients.add(client);
        client.retain();
    });
    queries = new Set();
    client.group.queriesByID.forEach(function(query) {
        queries.add(query);
        query.retain();
    });
    clientsQueries = new Map();
    clients.forEach(function(client) {
        var     queries;
        queries = new Set();
        clientsQueries.set(client, queries);
        client.queries.forEach(function(query) {
            queries.add(query);
        });
    });
    queriesClients = new Map();
    queries.forEach(function(query) {
        var     clients;
        clients = new Set();
        queriesClients.set(query, clients);
        query.clients.forEach(function(client) {
            clients.add(client);
        });
    });

    // for all queries, capture the ready to update condition and insert ourselves
    // in the processing queue
    
    queriesReadyToUpdate = new Map();
    queriesDoneWithResults = new Map();
    queries.forEach(function(query) {
        var     doneWithResults;
        
        doneWithResults = new Condition();
        queriesDoneWithResults.set(query, doneWithResults);
        queriesReadyToUpdate.set(query, query.readyToUpdate);
        query.readyToUpdate = doneWithResults;
    });

    // for all clients, capture the last push completed condition and insert
    // ourselves in the push queue

    lastPushesCompleted = new Map();
    currentPushesCompleted = new Map();
    clients.forEach(function(client) {
        var     currentPushCompleted;
        
        currentPushCompleted = new Condition();
        currentPushesCompleted.set(client, currentPushCompleted);
        lastPushesCompleted.set(client, client.lastPushCompleted);
        client.lastPushCompleted = currentPushCompleted;
    });
    
    // create a list of query result promises, indexed by the query id

    queryPromises = new Map();
    
    // create a list of notified clients. this is used to ensure clients are
    // notified no more than once
    
    notifiedClients = new Set();

    // resolve any conflict between the client and pending deletions & updates
    // any correction is done in place in clientDeletions and clientUpdates

    Sync.resolveConflicts(client, sequence, deletions, updates);

    // fix dangling references
    
    relations = Sync.resolveDanglingReferences(client, sequence, creations, deletions, updates);

    // actuate all events in the snapshot
    
    creations.forEach(function(creation) {
        creation.actuate(snapshot);
    });
    deletions.forEach(function(deletion) {
        deletion.actuate(snapshot);
    });
    updates.forEach(function(update) {
        update.actuate(snapshot);
    });

    // asynchronously commit the creations to the store
    
    creationsCommittedPromise = Creation.commitToStore(client, creations);

    // asynchronously commit the client deletions to the store now that conflicts have been resolved

    deletionsCommittedPromise = Deletion.commitToStore(client, deletions);

    // schedule a commit of the client updates to the store now that conflicts have been resolved
    // we wait a little before committing the updates so that future updates have a chance to
    // cancel this save.
    
    updatesCommittedPromise = new Timer(Update.updateCommitLag).wait()
    .then(function(result) {
        Update.commitToStore(client, updates);
    });

    // return the state in an object
    
    return {
        snapshot: snapshot,
        creations: creations,
        deletions: deletions,
        updates: updates,
        relations: relations,
        creationsCommittedPromise: creationsCommittedPromise,
        deletionsCommittedPromise: deletionsCommittedPromise,
        updatesCommittedPromise: updatesCommittedPromise,
        clients: clients,
        queries: queries,
        clientsQueries: clientsQueries,
        queriesClients: queriesClients,
        queriesReadyToUpdate: queriesReadyToUpdate,
        queriesDoneWithResults: queriesDoneWithResults,
        lastPushesCompleted: lastPushesCompleted,
        currentPushesCompleted: currentPushesCompleted,
        queryPromises: queryPromises,
        notifiedClients: notifiedClients,
    };
};

//

Sync.cleanupAfterProcessing = function(processingState)
{
    processingState.creations.forEach(function(creation) {
        creation.release();
    });
    processingState.deletions.forEach(function(deletion) {
        deletion.release();
    });
    processingState.updates.forEach(function(update) {
        update.release();
    });
    processingState.relations.forEach(function(relation) {
        relation.release();
    });
    processingState.clients.forEach(function(client) {
        client.release();
    });
    processingState.queries.forEach(function(query) {
        query.release();
    });

    processingState.snapshot.release();
};

// Sync.resolveConflicts = function(client, sequence, deletions, updates)
// returns nothing
// synchronous

Sync.resolveConflicts = function(client, sequence, deletions, updates)
{
    var     deletion;
    var     update;
    var     object;
    var     i;
    
    // iterate over the client deleted objects
    
    for(i=0; i<deletions.length; i++) {
        
        deletion = deletions[i];
        
        // make sure the deleted object exists

        object = deletion.object;
        if (!object.doesExist(sequence)) {

            // the object is already marked for deletion. drop it.
            
            deletion.release();
            deletions.splice(i--, 1);
            continue;
        }
        
        // any reference to the deleted object is left dangling
        // dangling references are corrected lazily in memory (see getRelations)
    }
    
    // iterate over the client updates
    
    for(i=0; i<updates.length; i++) {

        update = updates[i];
        object = update.object;
        
        // make sure the updated object exists
        
        if (!object.doesExist(sequence)) {
            update.release();
            updates.splice(i--, 1);
            continue;
        }
        
        update.values.forEach(function(value, key) {
            var     lastUpdateVersion;
            
            lastUpdateVersion = object.updateVersions.get(key);
    
            // check if the client update is more recent that the last update seen by the server
            
            if ((lastUpdateVersion !== undefined) && (lastUpdateVersion > update.version)) {
                
                // the server update is more recent than the client update for this key
                // the client key update can be dropped
                // the client is out-of-sync and will receive an update for the newer value
                
                update.values.delete(key);
            }
        });
        
        // if no more keys are left in this update, the entire update should be dropped
        
        if (update.values.size === 0) {
            update.release();
            updates.splice(i--, 1);
            continue;
        }
        
        // the client update is more recent. we keep it.
    }
};

// Sync.resolveDanglingReferences = function(client, sequence, creations, deletions, updates)
// fixes dangling references from loaded objects. dangling references in unloaded objects
// are fixed lazily in getFixedFetchList.
// synchronous
// returns a <Set> of retained relations

Sync.resolveDanglingReferences = function(client, sequence, creations, deletions, updates)
{
    // nullify references to the deleted objects
    
    Sync.nullifyDeletedReferences(client, sequence, deletions, updates);
    
    // fix any dangling references in creations and updates.
    // dangling references are fixed and changed to null references "in place".

    return Sync.validateDirectReferences(client, sequence, creations, updates);
};

// Sync.nullifyDeletedReferences = function(client, sequence, deletions, updates)
// nullifies all loaded references to deleted objects and generate an update
// if necessary.
// synchronous
// returns nothing

Sync.nullifyDeletedReferences = function(client, sequence, deletions, updates)
{
    // for each deletions, set all back-pointers to null reference.
    // generate a synthetic update to be added to the update list
    // unless there's an update for that key in the update list
    
    deletions.forEach(function(deletion) {
        deletion.references.forEach(function(count, object) {
            object.forEachRelation(sequence, function(key, value) {
                
                var     found;
                var     update;
                var     values;
                
                // set the reference to null
                
                object.setValue(key, Reference.nullReference, sequence);
                
                // look for an update to the same object. Array search is
                // O(n), which could be improved by using a <Map>
                
                found = updates.some(function(update) {
                    if (update.object != object)
                        return false;
                    if (!update.values.has(key))
                        update.addValue(key, Reference.nullReference);
                    return true;
                });
                if (!found) {
                    values = new Map([[key, Reference.nullReference]]);
                    update = new Update(null, object, 0, values);
                    updates.push(update.retain());
                }
            });
        });
    });
};

// Sync.validateDirectReferences = function(client, sequence, creations, updates)
// validates that all references passed in creations and updates are valid
// dangling reference are fixed "in place" by patching the creation or update.
// synchronous
// returns a list of retained relations

Sync.validateDirectReferences = function(client, sequence, creations, updates)
{
    var     objects;

    objects = new Set();
    creations.forEach(function(creation) {
        creation.forEachRelation(function(reference, key) {
            var     object;
            
            object = reference.validate(client, sequence, function() {
                
                // we fix the object value at sequence 0
                // see discussion in getClientCreations()
                
                creation.object.setValue(key, Reference.nullReference, 0);
                
                creation.fixed = true;
                creation.fixedKeys.push(key);
            });
            if (object !== null)
                objects.add(object);
        });
    });
    updates.forEach(function(update) {
        update.forEachRelation(function(reference, key) {
            var     object;
            
            object = reference.validate(client, sequence, function() {
                update.values.set(key, Reference.nullReference);
                update.fixed = true;
                update.fixedKeys.push(key);
            });
            if (object !== null)
                objects.add(object);
        });
    });
    return objects;
}

// Sync.processQueries = function(client, processingState)
// asynchronously generate the events to push to clients and pushes those events to
// the appropriate clients.
// returns a promise that resolves when all is done

Sync.processQueries = function(client, processingState)
{
    return Promise.evaluate(function() {
        
        // retain the snapshot before we start the process
        
        processingState.snapshot.retain();
        
        // each live query needs to process the changes and determine which events needs to
        // be pushed. this is a CPU intensive job. for this reason and to minimize the impact
        // on the overall server responsiveness, each query job is processed independently and
        // asynchronously.
        
        // each query job that yields events notifies the query clients that events are to
        // be pushed. this lazy notification scheme improves server scalability, as only clients
        // that are affected by the changes take processing time.

        processingState.queries.forEach(function(query) {
            
            var     promise;

            // asynchronously compute and push the query events, and remember the promise
            // the computation must imperatively be deferred, as it depends on all
            // query promises to be created.

            promise = Sync.processQuery(client, query, processingState);
            processingState.queryPromises.set(query.id, promise);
        });

        // return an aggregate promise that resolves when all queries are done computing and
        // pushing events
        
        return Promise.settle(processingState.queryPromises);
    })
    
    // wait for all queries to be done processing and pushing events

    .then(function(queryResults) {

        // release all the query results

        queryResults.forEach(function(queryResult) {

            queryResult.creations.forEach(function(creation) {
                creation.release();
            });
            queryResult.deletions.forEach(function(deletion) {
                deletion.release();
            });
            queryResult.updates.forEach(function(update) {
                update.release();
            });
        });

        // we're done. release the snapshot
        
        processingState.snapshot.release();

        return true;
    })
    
    .catch(function(e) {
        console.log('processQueries: ', e);
    });
};


Sync.processQuery = function(client, query, processingState)
{
    var     snapshot;
    var     sequence;
    var     creations;
    var     deletions;
    var     updates;
    var     queryClients;
    var     clientsQueries;
    var     queriesReadyToUpdate;
    var     queriesDoneWithResults;
    var     lastPushesCompleted;
    var     currentPushesCompleted;
    var     queryPromises;
    var     notifiedClients;
    var     matchingCreations;
    var     matchingDeletions;
    var     matchingUpdates;

    snapshot = processingState.snapshot;
    sequence = snapshot.sequence;
    creations = processingState.creations;
    deletions = processingState.deletions;
    updates = processingState.updates;
    queryClients = processingState.queriesClients.get(query);
    clientsQueries = processingState.clientsQueries;
    queriesReadyToUpdate = processingState.queriesReadyToUpdate;
    queriesDoneWithResults = processingState.queriesDoneWithResults;
    lastPushesCompleted = processingState.lastPushesCompleted;
    currentPushesCompleted = processingState.currentPushesCompleted;
    queryPromises = processingState.queryPromises;
    notifiedClients = processingState.notifiedClients;

    matchingCreations = new Set();
    matchingDeletions = new Set();
    matchingUpdates = new Set();

    // ensure we return immediately and defer the processing
    // this is necessary to ensure proper function, as pushEventsToClient
    // expects all the query result promises to be created

    return Timer.defer()
    
    // then wait for our turn in the query processing queue

    .then(function() {
        return queriesReadyToUpdate.get(query).wait();
    })
    
    // we're now ready to update the query results
    
    .then(function() {
        var     i;
        var     creation;
        var     deletion;
        var     update;
        var     object;
        var     didQualify;
        var     willQualify;

        // iterate over all creations, identify which are qualifying for this query

        for(i=0; i<creations.length; i++) {
            creation = creations[i];
            object = creation.object;
            if (query.qualifies(object, sequence)) {
                creation.qualify(query);
                matchingCreations.add(creation.retain());
            }
        }
        
        // iterate over all updates
        // identify which are qualifying or disqualifying for this query

        for(i=0; i<updates.length; i++) {
            update = updates[i];
            object = update.object;
            didQualify = query.qualified.has(object);
            willQualify = query.qualifies(object, sequence);
            if (didQualify && !willQualify) {
                update.disqualify(query);
                matchingUpdates.add(update);
            }
            if (!didQualify && willQualify) {
                update.qualify(query);
                matchingUpdates.add(update);
            }
        }
        
        // iterate over all deletions, identify which are disqualifying

        for(i=0; i<deletions.length; i++) {
            deletion = deletions[i];
            object = deletion.object;
            if (query.qualifies(object, sequence)) {
                deletion.disqualify(query);
                matchingDeletions.add(deletion);
            }
        }
        
        // notify the query clients that haven't been notified yet there are events to push

        queryClients.forEach(function(client) {
            var     queries;
            var     lastPushCompleted;
            var     currentPushCompleted;
            
            if (!notifiedClients.has(client)) {
                notifiedClients.add(client);
                queries = clientsQueries.get(client);
                lastPushCompleted = lastPushesCompleted.get(client);
                currentPushCompleted = currentPushesCompleted.get(client);
                Sync.pushEventsToClient(client, snapshot, queries, queryPromises, deletions, updates, lastPushCompleted, currentPushCompleted);
            }
        });

        // we're done updating the query. Signal the next update it can proceed
        
        queriesDoneWithResults.get(query).signal();
        
        // return the query results

        return {
            creations: matchingCreations,
            updates: matchingUpdates,
            deletions: matchingDeletions,
        };
    })
    
    .catch(function(reason) {
        
console.log('ERROR PUSH '+reason);
        return Promise.reject(reason);
    });
};

// Sync.getChangeFetchList = function(client, change, snapshot)
// returns a promise that resolves to the list of objects directly and indirectly reached
// from the change that are not already in the client working set.
// any dangling reference encountered along the way is fixed.
// the list is a <Set> of retained <SyncedObjects>

Sync.getChangeFetchList = function(client, change, snapshot)
{
    var     promises;
    var     exclude;
    var     fixes;
    var     lists;
    var     fetchList;
    
     return Promise.evaluate(function() {
        promises = [];
        exclude = new Set();
        fixes = [];
        
        // for each object of the qualified set, get a list of recursive
        // relations. collect all the dangling reference fix we run into along the way.
        
        change.forEachRelation(function(reference, key) {
            promises.push(SyncedObject.getFixedFetchList(client, reference, snapshot, change.object, key, exclude, function(update) {
console.log('fixing ', update.object.reference.globalID);
                fixes.push(update);
            }));
        });

        // wait for all the results
    
        return Promise.settle(promises);
    })
    
    .then(function(result) {

        lists = result;
        
        fetchList = new Set();
        
        // compile all lists into a single one
        
        lists.forEach(function(list) {
            list.forEach(function(object) {
                
                // filter out any reference to the changed object
                
                if (object == change.object)
                    object.release();
                else
                    fetchList.add(object);
            });
        });
        
        // asynchronously commit the fix updates for the dangling references we found
        
        Update.commitToStore(client, fixes);
        
        // we're done

        return fetchList;
    });
};


// Sync.pushEventsToClient = function(client, snapshot, queries, queryPromises, deletions, updates, lastPushCompleted, currentPushCompleted)
// push to a client all the events matching the client queries
// return a promise that resolves when all events have been pushed
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
//                  "version": #{version},
//                  "values": [
//                      [ "{property name}", "{string value}" | #{number value} ]
//                      [ "{relation name}",
//                          {
//                              "type": "global",
//                              "subclass": "{class name}",
//                              "id": "{object id}"
//                      } ]
//                      ...
//                  ],
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
//                  "version": #{version},
//                  "values": [
//                      [ "{property name}", "{string value}" | #{number value} ]
//                      [ "{relation name}",
//                          {
//                              "type": "global",
//                              "subclass": "{class name}",
//                              "id": "{object id}"
//                      } ]
//                      ...
//                  ]
//              }
//              ...
//          ]
//      },
//      sequence: #{sequence}

Sync.pushEventsToClient = function(client, snapshot, queries, queryPromises, deletions, updates, lastPushCompleted, currentPushCompleted)
{
    var     sequence;
    var     matchingCreations;
    var     matchingDeletions;
    var     matchingUpdates;
    var     queryResultPromises;
    var     queryResults;
    var     fetchListPromises;
    var     fetchLists;
    var     index;

    // wait for all the client queries to finish publishing their results

    Promise.evaluate(function() {
        
        // build a list of all the client queries

        queryResultPromises = [];
        queries.forEach(function(query) {
            queryResultPromises.push(queryPromises.get(query.id));
        });

        return Promise.settle(queryResultPromises);
    })
    
    // then build a list of events to report to the client
    
    .then(function(result) {

        queryResults = result;

        sequence = snapshot.sequence;

        // we have all the query results. some creations, deletions and updates
        // may be redundant. let's build a list of unique events.

        matchingCreations = new Set();
        matchingDeletions = new Set();
        matchingUpdates = new Set();
        
        queryResults.forEach(function(queryResult) {
            queryResult.creations.forEach(function(creation) {
                matchingCreations.add(creation);
            });
            queryResult.deletions.forEach(function(deletion) {
                matchingDeletions.add(deletion);
            });
            queryResult.updates.forEach(function(update) {
                matchingUpdates.add(update);
            });
        });

        // let's also report updates to objects that are in the client working set
        
        updates.forEach(function(update) {
            if (client.workingSet.has(update.object)) {
                matchingUpdates.add(update);
            }
        });

        // ... and deletions of objects that are in the client working set
        
        deletions.forEach(function(deletion) {
            if (client.workingSet.has(deletion.object)) {
                matchingDeletions.add(deletion);
            }
        });
        
        // now identify all objects that need to be fetched by the client

        fetchListPromises = [];
        matchingCreations.forEach(function(creation) {
            fetchListPromises.push(Sync.getChangeFetchList(client, creation, snapshot));
        });
        matchingUpdates.forEach(function(update) {
            fetchListPromises.push(Sync.getChangeFetchList(client, update, snapshot));
        });

        return Promise.settle(fetchListPromises);
    })

    // then make sure we're done pushing the previous update to the client
    // this guarantees in order reception of pushed updates
    
    .then(function(result) {
        
        fetchLists = result;
        return lastPushCompleted.wait();
    })
    
    // then make sure all created objects are committed to the store
    // as we need their object ids
    
    .then(function() {
        
        var     objectsCreated;

        objectsCreated = [];
        matchingCreations.forEach(function(creation) {
            objectsCreated.push(creation.object.created.wait());
        });

        return Promise.settle(objectsCreated);
    })

    // then compose and push the client messages
    
    .then(function(result) {
        
        // push the creation messages

        index = 0;
        matchingCreations.forEach(function(creation) {
            
            var     object;
            
            object = creation.object;

            // we don't need to push this creation if the client originated it,
            // unless we had to fix dangling references
            // if that's the case, we push updates for the keys that had to be corrected
            
            if (creation.client == client) {
                index++;
                if (!creation.fixed)
                    return;
                client.push({
                   updates: {
                        subclass: object.reference.subclass,
                        id: object.reference.globalID,
                        version: object.version,
                        values: creation.fixedKeys.map(function(key) {
                            return [ key, Request.composeValue(Reference.nullReference) ];
                        })
                   }
                });
                return;
            }
            
            client.addObjectToWorkingSet(object);
            client.push({
                creations: {
                    subclass: object.reference.subclass,
                    id: object.reference.globalID,
                    version: object.version,
                    values: Request.composeObjectValues(object, sequence),
                    qualifying: Request.composeQueryList(creation.qualifying),
                    fetch: Request.composeObjectList(fetchLists[index++], sequence)
                }
            });
        });
        
        // push the deletion messages

        matchingDeletions.forEach(function(deletion) {
            
            var     object;
            
            object = deletion.object;
            
            // we don't need to push the deletion if the client originated it
            
            if (deletion.client == client)
                return;
                
            client.removeObjectFromWorkingSet(object);
            client.push({
                deletions: {
                    subclass: object.reference.subclass,
                    id: object.reference.globalID,
                    disqualifying: Request.composeQueryList(deletion.disqualifying)
                }
            });
        });

        // push the update messages

        matchingUpdates.forEach(function(update) {
            
            // we don't need to push this update if the client originated it
            // unless we had to fix it because it was a dangling reference
            
            if (update.client == client) {
                index++;
                if (!update.fixed)
                    return;
                client.push({
                    updates: {
                        subclass: update.object.reference.subclass,
                        id: update.object.reference.globalID,
                        version: update.version,
                        values: update.fixedKeys.map(function(key) {
                                return [ key, Request.composeValue(Reference.nullReference) ];
                            }),
                    }
                });
                return;
            }
                
            client.push({
                updates: {
                    subclass: update.object.reference.subclass,
                    id: update.object.reference.globalID,
                    version: update.version,
                    values: Request.composeUpdateValues(update),
                    qualifying: Request.composeQueryList(update.qualifying),
                    disqualifying: Request.composeQueryList(update.disqualifying),
                    fetch: Request.composeObjectList(fetchLists[index++], sequence)
                }
            });
        });

        // signal we're done pushing
        
        currentPushCompleted.signal();
        
        return true;
    })

    .catch(function(reason) {
       console.log("pushEventsToClient ERROR");
console.log(reason);
reason.rejected.forEach(function(e) {
    console.log(e);
});
       return Promise.reject(reason);
    });
};