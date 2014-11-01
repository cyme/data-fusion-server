module.exports = Query;

var Condition = require('sync-utils').Condition;

var Retainable = require('./retainable');
var SyncedObject = require('./synced_object');
var Update = require('./update');
var ServerError = require('./server_error');
var Store = require('./store');

// use ES5 and the es6-shim library from Paul Miller until node catches up to a recent enough version
// of V8 (3.27.x or newer) that better supports the ES6 collections

require('es6-shim');



// Query inherits from Retainable
// {
//      group:              <Group>,
//      id:                 "<query id>",
//      subclass:           "<class name>",
//      predicate:          FOR FUTURE USE,
//      readyToUpdate:      <Condition>,        /* processing queue */
//      clients:            <Set>,              /* WEAK list of watching <Client> */
//      qualified:          <Set>,              /* qualifying objects */
// }

function Query(group, subclass)
{
    Retainable.call(this);
    
    // make sure a query with the same predicate does not exist
    
    if (group.queriesBySubclass.has(subclass))
        throw new ServerError("Query: query already exists");

    this.group = group;
    this.id = (Query.queryID++).toString();
    this.subclass = subclass;
    this.clients = new Set();
    this.readyToUpdate = Condition.trueCondition;

    this.qualified = null;
}

Query.prototype = Object.create(Retainable.prototype);
Query.prototype.constructor = Query;

Query.prototype.register = function()
{
    this.group.queriesByID.set(this.id, this);
    this.group.queriesBySubclass.set(this.subclass, this);
};

Query.prototype.unregister = function()
{
    this.group.queriesByID.delete(this.id);
    this.group.queriesBySubclass.delete(this.subclass);

    this.clients.forEach(function(client) {
        client.queries.delete(this);
    }, this);
    if (this.qualified !== null)
        this.qualified.forEach(function(object) {
            object.release();
        });
};

Query.prototype.addClient = function(client)
{
    this.clients.add(client);
    client.queries.add(this);
    this.retain();
};

Query.prototype.removeClient = function(client)
{
    this.clients.delete(client);
    client.queries.delete(this);
    this.release();
};

Query.prototype.find = function(client, snapshot)
{
    var     query;
    var     sequence;
    var     stage;
    var     objects;
    var     relationLists;

    query = this;
    stage = 0;
    sequence = snapshot.sequence;

    // if a qualified set is already available, nothing more to do
    
    if (this.qualified !== null)
        return Promise.resolve(true);

    // initiate a query operation on the store

    return Promise.evaluate(function() {

        var     operation;

         operation = {
            type: "QUERY",
            subclass: query.subclass
        };
        return Store.issueOperation(client, operation);
    })
    
    // then account for pending server changes that may not have committed
    // to the store yet

    .then(function(result) {
        var     group;
        var     creations;
        var     deletions;
        var     updates;
        var     creationsCommitted;

        query.qualified = new Set();
        objects = result.objects;
        group = client.group;
        creationsCommitted = [];
        creations = group.pendingCreations.get(query.subclass);
        if (creations !== undefined) {
            creations.forEach(function(creation) {
                var     object;
                object = creation.object;
                if (!objects.has(object) && query.qualifies(object, sequence)) {
                    objects.add(object.retain());
                    creationsCommitted.push(object.created);
                }
            });
        }

        deletions = group.pendingDeletions.get(query.subclass);
        if (deletions !== undefined) {
            deletions.forEach(function(deletion) {
                var     object;
                object = deletion.object;
                if (objects.has(object) && query.qualifies(object, sequence)) {
                    objects.delete(object);
                    object.release();
                }
            });
        }

        updates = group.pendingUpdates.get(query.subclass);
        if (updates !== undefined) {
            updates.forEach(function(updates, key) {
                updates.forEach(function(update, key) {
                    var     object;
                    object = update.object;
                    if (!objects.has(object) && query.qualifies(object, sequence))
                        objects.add(object.retain());
                    if (objects.has(object) && !query.qualifies(object, sequence)) {
                        objects.delete(object);
                        object.release();
                    }
                });
            });
        }

        // wait for all the creations we've added to be committed so that
        // they are assigned an object id. qualified sets must consist of
        // objects backed by the store.

        Promise.settle(creationsCommitted);
    })
    
    // then create the qualified set from the results
    
    .then(function(result) {
        var     promises;
        var     exclude;

        promises = [];
        exclude = new Set();

        stage = 1;

        objects.forEach(function(object) {

            // add each object found to the qualified set
            // from this point on, we are tracking changes to those objects
          
            query.qualified.add(object);
        });

        // we're done
        
        return true;
    })
    
    // if any error was encountered along the way, deal with it here
    
    .catch(function(error) {
        
        switch(stage) {
            case 2:
                // error was encountered after loading all relations
                
                relationLists.forEach(function(list) {
                    list.forEach(function(object) {
                        object.release();
                    });
                });
                break;
                
            case 1:
                // error was encountered while loading the relations
                
                error.resolved.forEach(function(list) {
                    if (list === undefined)
                        return;
                    list.forEach(function(object) {
                        object.release();
                    });
                });
                error = error.rejected.find(function(error) {
                    return (error !== undefined);
                });
                break;
                
            case 0:
                // error was encountered before loading all relations
                
                break;
        }
        console.error('error downloading objects ', error);
        return Promise.reject(error);
    });
};

// returns a bool stating whether the object in the state captured by a snapshot
// qualifies for the query

Query.prototype.qualifies = function(object, sequence)
{
    return ((this.subclass == object.reference.subclass) && object.doesExist(sequence));
};

// returns an unretained query that matches the subclass
// a new query is created if none exist for the subclass

Query.getQueryForSubclass = function(group, subclass)
{
    var     query;
    
    // check if a query with the same predicate already exists
    
    query = group.queriesBySubclass.get(subclass);

    // if no matching query can be found, then create a new one
    
    if (query !== undefined)
        return query;
        
    return new Query(group, subclass);
};


// Query.getFetchList = function(client, qualified, snapshot)
// returns a promise that resolves to the list of objects directly and indirectly
// reached from the qualified objects that are not already in the client working set.
// any dangling reference encountered along the way is fixed.
// the list is a <Set> of retained <SyncedObjects>

Query.getFetchList = function(client, qualified, snapshot)
{
    var     sequence;
    var     promises;
    var     exclude;
    var     fixes;
    var     lists;
    var     fetchList;

    return Promise.evaluate(function() {
        sequence = snapshot.sequence;
        promises = [];
        exclude = new Set();
        fixes = [];

        qualified.forEach(function(object) {

            // for each object of the qualified set, get a list of recursive
            // relations. collect all the dangling reference fix we run into along the way.
 
            object.forEachRelation(sequence, function(reference, key) {
                promises.push(SyncedObject.getFixedFetchList(client, reference, snapshot, object, key, exclude, function(update) {
                    fixes.push(update);
                }));
            });

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
                
                // skip any qualified object
                
                if (qualified.has(object))
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

// static functions, variables and constants

Query.queryID = 1;
