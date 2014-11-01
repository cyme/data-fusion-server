module.exports = Update;

var Condition = require('sync-utils').Condition;

var Change = require('./change');
var Reference = require('./reference');
var ServerError = require('./server_error');
var Store = require('./store');


// use ES5 and the es6-shim library from Paul Miller until node catches up to a recent enough version
// of V8 (3.27.x or newer) that better supports the ES6 collections

require('es6-shim');



// Update inherits from Change
// {
//      committed:      <Condition>,
//      committing:     bool,
//      sequence:       #{sequence},
//      version:        #{sequence},        /* sequence of base version, used for conflict resolution */
//      older:          <Map>,              /* [ "{key}", <Update> ] pairs for prior same-key updates */
//      newer:          <Map>,              /* [ "{key}", <Update> ] pairs for subsequent same-key updates */ */
//      fixed:          bool,               /* client submitted dangling reference */
//      fixedKeys:      <Array>,            /* <Array> of fixed keys */
//      values:         <Map>,              /* [ "{key}",  #{value} | "{value}" | <Reference> ] pairs */
//      qualifying:     <Set>,              /* WEAK list of qualifying queries */
//      disqualifying:  <Set>               /* WEAK list of disqualifying queries */
// }

function Update(client, object, version, values)
{
    Change.call(this, client, object);
    
    this.committed = new Condition();
    this.committing = false;

    this.sequence = 0;
    this.version = version;

    this.older = new Map();
    this.newer = new Map();
    
    this.fixed = false;
    this.fixedKeys = [];
    
    this.values = values;

    this.qualifying = new Set();
    this.disqualifying = new Set();
}

Update.prototype = Object.create(Change.prototype);
Update.prototype.constructor = Update;

Update.prototype.forEachValue = function(fn)
{
    this.values.forEach(function(value, key) {
        fn(value, key);
    });
};

Update.prototype.forEachRelation = function(fn)
{
    this.values.forEach(function(value, key) {
        if (!Reference.isReference(value))
            return;
        fn(value, key);
    });
};

Update.prototype.addValue = function(key, value)
{
    if (this.values.has(key))
        throw new ServerError('Update.addValue: value already set');
    this.values.add(key, value);
    this.fixed = true;
    this.fixedKeys.push(key);
};

Update.prototype.actuate = function(snapshot)
{
    var     sequence;
    var     object;
    var     group;
    var     subclass;
    var     updates;
    var     newer;
    var     older;

    sequence = snapshot.sequence;
    this.sequence = sequence;
    
    object = this.object;

    object.version++;
    object.lastUpdateSequence = sequence;
    this.values.forEach(function(value, key) {
        object.setValue(key, value, sequence);
        object.updateVersions.set(key, object.version);
    });
    snapshot.addUpdate(this);

    group = object.group;
    subclass = group.pendingUpdates.get(object.reference.subclass);
    if (subclass === undefined) {
        subclass = new Map();
        group.pendingUpdates.set(object.reference.subclass, subclass);
    }

    updates = subclass.get(object.reference.globalID);
    if (updates === undefined) {
        updates = new Map();
        subclass.set(object.reference.globalID, updates);
    }

    // for each key, insert the update in the ordered pending update queue

    this.values.forEach(function(value, key) {
        newer = null;
        older = updates.get(key);
        if (older === undefined)
            older = null;
        while ((older !== null) && (older.sequence > sequence)) {
            newer = older;
            older = older.older.get(key);
        }
        
        if (older !== null) {
            this.older.set(key, older);
            older.newer.set(key, this);
        }
        if (newer !== null) {
            this.newer.set(key, newer);
            newer.older.set(key, this);
        } else
            updates.set(key, this);
            
        this.retain();
    }, this);
};

Update.prototype.didCommit = function()
{
    var     object;
    var     sequence;
    var     group;
    var     subclass;
    var     updates;

    object = this.object;
    sequence = this.sequence;
    
    this.committing = false;
    
    this.values.forEach(function(value, key) {
        var     newer;
        var     older;
        
        object.commitSequences.set(key, sequence);
    
        newer = this.newer.get(key);
        older = this.older.get(key);
        if (newer !== undefined) {
            if (older !== undefined)
                newer.older.set(key, older);
            else
                newer.older.delete(key);
        } else {
            group = object.group;
            subclass = group.pendingUpdates.get(object.reference.subclass);
            updates = subclass.get(object.reference.globalID);
            updates.delete(key);
            if (updates.size === 0) {
                subclass.delete(object.reference.globalID);
                if (subclass.size === 0)
                    group.pendingUpdates.delete(object.reference.subclass);
            }
        }
        if (older !== undefined) {
            if (newer !== undefined)
                older.newer.set(key, newer);
            else
                older.newer.delete(key);
        }
            
        this.release();
    }, this);

    this.committed.signal();
};

Update.prototype.qualify = function(query, snapshot)
{
    this.qualifying.add(query);
    query.qualified.add(this.object.retain());
};

Update.prototype.disqualify = function(query)
{
    this.disqualifying.add(query);
    query.qualified.delete(this.object);
    this.object.release();
};

// returns a promise of an update operation, which instructs to commit the update to the store,
// or null if no update needs to be saved

Update.prototype.getStoreOperation = function()
{
    var     operation;
    var     object;
    var     values;
    var     waitForOlder;
    
    object = this.object;
    
    // if the object has been deleted, there's nothing to commit. return a null promise.

    if (object.isDeleted())
        return null;

    waitForOlder = [];
    
    // create 1) a list of [key, value] pairs to commit
    // and 2) a list of pending older updates to wait for
    
    values = new Map(this.values.entries());
    this.values.forEach(function(value, key) {
        var     sequence;
        var     older;
        
        // we don't to commit a value if:
        // 1- the committed store value is more recent than this update OR
        // 2- a more recent update is waiting to be committed

        sequence = object.commitSequences.get(key);
        if (((sequence !== undefined) && (sequence > this.sequence)) || (this.newer.get(key) !== undefined)) {
            values.delete(key);
        }
        
        // remember any older pending update to the same key
        
        older = this.older.get(key);
        if (older !== undefined)
            waitForOlder.push(older.committed.wait());
    }, this);

    // if no values are left to update, there's nothing to commit. return a null promise
    
    if (values.size === 0)
        return null;
        
    // create the update operation
    
    operation = {
        type: "UPDATE",
        update: this,
        object: object,
        version: this.version,
        values: values
    };

    // if one or more older update is pending, we need to wait for them to ensure the store
    // reflects the latest update.
    // once all older updates have completed, we return a promise for an update operation

    if (waitForOlder.length > 0)
        return Promise.all(waitForOlder).then(function(result) {
            return operation;
        });

    // no older update is being saved. if the object hasn't been created in the store yet,
    // we return a promise for an update operation that resolves when the object is created.
    
    if (!this.object.created.set)
        return this.object.created.wait().then(function(result) {
            return operation;
        });
    
    // the object has already been created, we can return an simple operation object
    
    return Promise.resolve(operation);
};

// static functions, variables and constants


Update.updateCommitLag = 1*1000;

// Update.commitToStore(client, updates)
// asynchronously commit the client updates to the store
// returns a promise that resolves to the updates
// executes without side-effects: caller doesn't need to unwind when handling errors

Update.commitToStore = function(client, updates)
{
    var         operationPromises;

    return Promise.evaluate(function() {
        
        var         update;
        var         promise;
        var         i;

        operationPromises = [];

        for(i=0; i<updates.length; i++) {
    
            update = updates[i];
    
            // flag we're about to commit the update
    
            update.committing = true;
            
            // we don't need to commit the update if there's nothing to commit
            
            promise = update.getStoreOperation();
            if (promise === null) {
                update.didCommit();
                continue;
            }
            
            operationPromises.push(promise);
        }
                            
        // gather all the update store operations. This wait for any older update to
        // complete committing.
        // note that we can safely call Promise.all instead of Promise.settle
        // because Update.getStoreOperation has no side effect - and therefore
        // there's nothing to unwind in case of error

        return Promise.all(operationPromises);
    })
    
    // when done, commit the updates

    .then(function(updateOperations) {

        // we could re-validate the updates here by checking if they have been
        // overwritten or if the underlying object has been deleted in the meantime.
        // we're not doing it for simplicity
        
        return Store.issueBatchOperation(client, updateOperations);
    })
    
    // when done, mark the updates committed
    
    .then(function(batchResults) {

        batchResults.forEach(function(result) {
            var     update;
            
            update = result.operation.update;
console.log('COMMITTING ', update.object.reference.globalID);

            update.didCommit();
        });
        return updates;
    });
};