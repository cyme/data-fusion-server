module.exports = SyncedObject;

var Condition = require('sync-utils').Condition;

var Retainable = require('./retainable');
var Capture = require('./capture');
var Reference = require('./reference');
var SyncError = require('./sync_error');
var UserError = require('./user_error');
var ServerError = require('./server_error');
var Store = require('./store');


// SyncedObject inherits from Retainable
// {
//      group:              <Group>,
//      reference:          <Reference>,
//      created:            <Condition>,                            
//      loaded:             <Condition>,
//      error:              <SyncError>,                /* any error while loading */
//      creationSequence:   #{sequence},                /* 0 or creation sequence */
//      deletionSequence:   #{sequence},                /* 0 or deletion sequence */
//      captures:           <Map>,                      /* ["{key}", <Capture>] pairs */
//      commitSequences:    <Map>,                      /* ["{key}", #{sequence}] pairs */
//      updateVersions:     <Map>,                      /* ["{key}", #{version}] pairs */
//      version:            #{version},                 /* object version */
//      lastUpdateSequence: #{sequence},                /* sequence of last update */
//      references:         <Map>,                      /* [<SyncedObject>, #{count}] pairs */
// }

function SyncedObject(group, reference, created, loaded, isNew)
{
    Retainable.call(this);
    
    this.group = group;
    this.reference = reference;
    this.created = created;
    this.loaded = loaded;
    this.error = null;
    this.creationSequence = isNew ? -1 : 0;
    this.deletionSequence = 0;
    this.captures = new Map();
    this.commitSequences = new Map();
    this.version = 0;
    this.updateVersions = new Map();
    this.lastUpdateSequence = 0;
    this.references = new Map();
}

SyncedObject.prototype = Object.create(Retainable.prototype);
SyncedObject.prototype.constructor = SyncedObject;

SyncedObject.prototype.register = function()
{
    this.reference.register(this);
};

SyncedObject.prototype.unregister = function()
{
    var     self;

    self = this;
    self.reference.unregister();
    self.forEachRelation(self.lastUpdateSequence, function(reference, key) {
        var     relation;

        relation = reference.getValidObject();
        if (relation === null)
            return;
        relation.removeReference(self, key);
    });
};

SyncedObject.prototype.setValue = function(key, value, sequence)
{
    var     oldValue;
    var     relation;
    var     previous;
    var     next;
    var     capture;

    previous = null;
    next = this.captures.get(key);
    if (next === undefined)
        next = null;
        
    while ((next !== null) && (next.sequence <= sequence)) {
        previous = next;
        next = next.next;
    }
    
    if ((previous !== null) && (previous.sequence == sequence)) {
        oldValue = previous.value;
        previous.value = value;
    } else {
        capture = new Capture(value, sequence);
        capture.previous = previous;
        capture.next = next;
        if (previous === null)
            this.captures.set(key, capture);
        else {
            oldValue = previous.value;
            previous.next = capture;
        }
        if (capture.next)
            capture.next.previous = capture;
    }

    if (this.lastUpdateSequence <= sequence) {

        if (Reference.isReference(oldValue) && !oldValue.isNull()) {
            if (!this.group.dropReferenceUpdate(oldValue, this, key)) {
                relation = oldValue.getValidObject();
                if ((relation === null) || !relation.doesExist(sequence))
                    throw new ServerError("SyncedObject.setValue: inconsistent back pointers");
                relation.removeReference(this, key);
            }
        }

        if (Reference.isReference(value) && !value.isNull()) {
            relation = value.getValidObject();
            if ((relation !== null) && relation.doesExist(sequence)) {
                relation.addReference(this, key);
            } else {
                this.group.needReferenceUpdate(value, this, key);
            }
        }
    }
};

SyncedObject.prototype.getValue = function(key, sequence)
{
    var     capture;
    
    if (!this.doesExist(sequence))
        return undefined;
    capture = this.captures.get(key);
    if ((capture === undefined) || (capture.sequence > sequence))
        return undefined;
    while ((capture.next !== null) && (capture.next.sequence <= sequence))
        capture = capture.next;
    return capture.value;
};

SyncedObject.prototype.discardEarlierValues = function(key, sequence)
{
    var     capture;
    
    capture = this.captures.get(key);
    if (capture === undefined)
        capture = null;
    while ((capture !== null) && (capture.sequence < sequence))
        capture = capture.next;
    if (capture !== null)
        this.captures.set(key, capture);
    else
        this.captures.delete(key);
};

SyncedObject.prototype.forEachValue = function(sequence, fn)
{
    if (!this.doesExist(sequence))
        return;
    this.captures.forEach(function(capture, key) {
        var     value;
        value = this.getValue(key, sequence);
        if (value !== undefined)
            fn(value, key, Reference.isReference(value));
    }, this);
};

SyncedObject.prototype.forEachRelation = function(sequence, fn)
{
    if (!this.doesExist(sequence))
        return ;
    this.captures.forEach(function(capture, key) {
        var     value;
        value = this.getValue(key, sequence);
        if ((value !== undefined) && Reference.isReference(value))
            fn(value, key);
    }, this);
};

SyncedObject.prototype.addReference = function(object, key)
{
    var     count;

    count = this.references.get(object);
    if (count === undefined)
        count = 0;
    this.references.set(object, count+1);
console.log('addReference('+this.reference.globalID+') for ('+object.reference.globalID+', '+key+'): count now '+(count+1));
};

SyncedObject.prototype.removeReference = function(object, key)
{
    var     count;

    count = this.references.get(object);

console.log('removeReference('+this.reference.globalID+') for ('+object.reference.globalID+', '+key+'): count was '+count);

    if (count === undefined)
        throw new ServerError("SyncedObject.removeReference: inconsistent back pointers");
console.log("RR 3");
    if (count == 1)
        this.references.delete(object);
    else
        this.references.set(object, count-1);
};

SyncedObject.prototype.isDeleted = function()
{
    return (this.deletionSequence > 0);
};

SyncedObject.prototype.doesExist = function(sequence)
{
    if (this.creationSequence === -1)
        return false;
    return ((sequence >= this.creationSequence) &&
        ((this.deletionSequence === 0) || (this.deletionSequence <= sequence)));
};

SyncedObject.prototype.toString = function()
{
    return 'SyncedObject { class: '+this.reference.subclass+', id: '+this.reference.globalID+', refcount: '+this.refcount+' }';
};

// returns a load operation that instructs to load the object from the store

SyncedObject.prototype.getStoreOperation = function()
{
    return {
        type: "LOAD",
        object: this
    };
};

// static method that asynchronously loads an object given a reference
// returns a promise that resolves to a retained object

SyncedObject.load = function(client, reference, sequence)
{
    var     object;
    var     operation;

    return Promise.evaluate(function() {

        // check for null references
        
        if (reference == Reference.nullReference)
            return null;
            
        // check if the object is already instantiated

        object = reference.getObject();
        if (object !== null) {
            
            // the object is already instantiated
            // retain it and wait until it completes loading before proceeding
            
            object.retain();
console.log('SO: already instantiated/waiting '+object.reference.globalID);
            return object.loaded.wait().then(function() {
                
                var     error;
console.log('SO: done waiting '+object.reference.globalID);
                
                // if load failed, return the error

                error = object.error;
                if (error !== null) {
                    object.release();
                    return Promise.reject(error);
                }
                
                // if the object doesn't exist in the passed snapshot
                // return an error

                if ((sequence !== 0) && !object.doesExist(sequence)) {
                    object.release();
                    return Promise.reject(new UserError(SyncError.errorObjectNotFound));
                }
                    
                // the object exists. return it.
                
                return object;
            });
        }
            
        object = new SyncedObject(client.group, reference, Condition.trueCondition, new Condition(), false);
        
        // retain the object so that it remains registered
        
        object.retain();

        // load the object from the store
        
        operation = object.getStoreOperation();
        
        return Store.issueOperation(client, operation)
        
        // deal with loading errors here
        
        .catch(function(reason) {

console.log('SO.load error', reference.globalID);            
            object.error = reason;
            object.loaded.signal();
            object.release();
            return Promise.reject(reason);
        })
        
        // wait until the object is loaded
    
        .then(function(result) {
    
            // do back-pointer maintenance

            object.group.updateReference(object);

            // then signal anybody waiting for the object to load
            
            object.loaded.signal();
            
            // we're done

            return object;
        });
    });
};

// SyncedObject.getFixedFetchList = function(client, reference, snapshot, parent, key, exclude, fix)
// returns a promise that resolves to a <Set> of all direct and indirect relations that
// can be reached from an object that are not already part of the client working set.
// Additionally, any dangling relations encountered on the way is fixed, i.e. changed to
// a null reference and committed to the store.

SyncedObject.getFixedFetchList = function(client, reference, snapshot, parent, key, exclude, fix)
{
    var     sequence;

    sequence = snapshot.sequence;
    return SyncedObject.getFetchList(client, reference, sequence, parent, key, exclude, function(object, key, reference) {
        var     update;
        var     values;
        
        // a dangling reference was found at object[key] in the given snapshot.
        // let's change it to a null reference.
        // for that we create an <Update> and we actuate it.
        // note that this update does not need to be notified to clients as dangling
        // references are corrected before they are seen by clients.
        // note also that it is possible for 2 concurrent calls to getFixedFetchList in
        // different snapshots to both issue an update for the same dangling reference.
        // the write-after-write situation is handled properly by the <Update> logic
        // and this is harmless.

        values = new Map([[key, Reference.nullReference]]);
        update = new Update(null, object, 0, values);
        update.retain();
        update.actuate(snapshot);
        fix(update);
    });
};


// SyncedObject.getFetchList = function(client, reference, sequence, parent, key, exclude, fix)
// returns a promise that resolves to a <Set> of all direct and indirect relations that
// can be reached from an object that are not already part of the client working set.
// the function recursively loads objects as needed

SyncedObject.getFetchList = function(client, reference, sequence, parent, key, exclude, fix)
{
    var     list;
    var     recursiveRelations;
    var     object;

    // load the object so that we can explore its relations

    return SyncedObject.load(client, reference, sequence)
    
    // handle load error here
    
    .catch(function(error) {

        if (error.getReason() == SyncError.errorObjectNotFound) {
console.log('dangling', reference.globalID);

            // this is a dangling relation.
            // we return the a promise resolving to the null value so to inform the
            // caller

            return Promise.resolve(null);
        }
console.log('ERROR', reference.globalID);
        return Promise.reject(error);
    })
    
    // the object has succesfully loaded
    
    .then(function(result) {
        var     recursiveRelationsPromises;
        var     promise;

        // deal with dangling reference (see catch code above)
        // any dangling references encountered here is silently ignored
        // references provided by clients are verified in sync(), and dangling references
        // are corrected there.

        if (result === null) {
            if ((parent.getValue(key, sequence) !== Reference.nullReference) && (fix !== null)) {
                fix(parent, key, reference);
            }
            return new Set();
        }
        
        // if the object is already in the client working set, or if it has already been
        // explored (object graph cycle), we don't need to explore this node

        object = result;
        if (client.workingSet.has(object) || exclude.has(object)) {
            
            // release the objects as it was retained again by SyncedObject.load

            object.release();
            
            // return an empty set
console.log('in working set', reference.globalID);            
            return new Set();
        }
    
        // add the object to the exclusion list, so that we can detect cycles

        exclude.add(object);
        
        recursiveRelationsPromises = [];

        // recursively explore the relations of this node
        // each explored relation returns a promise which expands to the list of its relation
        // the promise gets added to the promise array

        object.forEachRelation(sequence, function(relationReference, key) {
            promise = SyncedObject.getFetchList(client, relationReference, sequence, object, key, exclude, fix);
            recursiveRelationsPromises.push(promise);
        });

        // wait for the relations to be explored

        return Promise.settle(recursiveRelationsPromises)
    
        // handle error in the recursion tree here
        
        .catch(function(error) {

            var     resolved;
            var     rejected;
            var     i;
        
console.log('getRelation recursion error ', error);        
            
            // release the object we just loaded
            
            object.release();

            // examine the reason object returned by Promise.settle
            // if any promise resolved to a relation list, all relations in the list must be released
            
            resolved = error.resolved;
            for(i=0; i<resolved.length; i++) {
                if (resolved[i] !== undefined)
                    resolved[i].forEach(function(relation) {
                        relation.release();
                    });
            }
            
            // return the first reason found
            
            rejected = error.rejected;
            for(i=0; i<rejected.length; i++) {
                if (rejected[i] !== undefined)
                    return Promise.reject(rejected[i]);
            }
            
            // be safe - to cover the case all rejected promises are undefined
console.log('getRelation SHOULD NOT GET HERE ');        
            
            return Promise.reject(undefined);
        })
        
        // the recursion succeeded
        // build a list of all the relations discovered under this node
        // the return value from getRelations is a promise of a promise that
        // resolves to this list
        
        // we do this in a nested rather than a flat then because we don't
        // want to execute this if a cycle was detected earlier
        
        .then(function(results) {
            
            var     index;

            recursiveRelations = results;
            list = new Set();
            index = 0;
            
            // add the base object to the list
            
            list.add(object);
            
            // add the recursive relations to the list
            
            object.forEachRelation(sequence, function(reference, key) {
                
                var     indirectRelations;
                
                indirectRelations = recursiveRelations[index++];
                
                indirectRelations.forEach(function(relation) {
                    
                    // check for duplicates in the list. the relation is added if it isn't already
                    // in the list. it's already been retained by SyncedObject.load. However if the
                    // object is already in the list, it should be released to balance out the prior
                    // retain.
    
                    if (!list.has(relation))
                        list.add(relation);
                    else
                        relation.release();
                });
            });

            return list;
        });
    });
};

