module.exports = Deletion;

var Change = require('./change');
var ServerError = require('./server_error');
var Store = require('./store');

// use ES5 and the es6-shim library from Paul Miller until node catches up to a recent enough version
// of V8 (3.27.x or newer) that better supports the ES6 collections

require('es6-shim');


// Deletion inherits from Change
// {
//      disqualifying:  <Set>               /* list of all disqualifying queries */
// }

function Deletion(client, object)
{
    Change.call(this, client, object);
    
    this.disqualifying = new Set();
}

Deletion.prototype = Object.create(Change.prototype);
Deletion.prototype.constructor = Deletion;

Deletion.prototype.actuate = function(snapshot)
{
    var     object;
    var     group;
    var     subclass;
    
    object = this.object;
    if (object.deletionSequence !== 0)
        throw new ServerError("Deletion.actuate: already actuated");
    object.deletionSequence = snapshot.sequence;
    
    group = object.group;
    subclass = group.pendingDeletions.get(object.reference.subclass);
    if (subclass === undefined) {
        subclass = new Map();
        group.pendingDeletions.set(object.reference.subclass, subclass);
    }
    subclass.set(object.reference.globalID, this.retain());
};

Deletion.prototype.didCommit = function()
{
    var     object;
    var     group;
    var     subclass;
    
    object = this.object;
    group = object.group;

    subclass = group.pendingDeletions.get(object.reference.subclass);
    subclass.delete(object.reference.globalID);
    this.release();
};

Deletion.prototype.disqualify = function(query)
{
    this.disqualifying.add(query);
    query.qualified.delete(this.object);
    this.object.release();
};

// returns a promise of the delete operation
// executes without side-effects: caller doesn't need to unwind when handling errors

Deletion.prototype.getStoreOperation = function()
{
    var     object;
    var     operation;
    var     updatesSaved;
    
    object = this.object;

    operation = {
        type: "DELETE",
        object: object,
    };
    
    // we need to wait for all updates to the object to be saved
    
    updatesSaved = [];
    object.updates.forEach(function(update) {
        
        // if the update is already saved, no wait needed
        
        if (update.saved.set)
            return;
            
        // if the update hasn't been saved yet and isn't in the process of being saved,
        // no wait needed as the object is now marked deleted and the update won't be saved
        
        if (!update.committing)
            return;
            
        // the update is being saved. we need to wait for the save to complete
        
        updatesSaved.push(update.saved.wait());
    });

    // if any update save is pending, return a promise for a delete operation that will resolve
    // when all the pending save complete

    if (updatesSaved.length > 0)
        return Promise.all(updatesSaved).then(function(result) {
            return operation;
        });
    
    // no update save is pending. if the object is still being created in the store, return a
    // promise for a delete operation that resolves when the object creation completes.
    // this could be optimized if object creations are deferred by canceling the object creation and deletions in the store
    
    if (!object.created.set) {
        return object.created.wait().then(function(result) {
            return operation;
        });
    }
    
    // the object has already been created in the store. We can simply return a delete operation
    
    return Promise.resolve(operation);
};

Deletion.prototype.toString = function()
{
    return 'Deletion { class: '+this.object.reference.subclass+', id: '+this.object.reference.globalID+' }';
};



// static functions, variables and constants



// Deletion.commitToStore(client, deletions)
// asynchronously delete the list of object passed from the store
// returns a promise that resolves to the deletions
// executes without side-effects: caller doesn't need to unwind when handling errors

Deletion.commitToStore = function(client, deletions)
{
    var     i;
    var     deletion;
    var     operationPromises;
    
    return Promise.evaluate(function() {
        
        operationPromises = new Array(deletions.length);
    
        for(i=0; i<deletions.length; i++) {
            
            deletion = deletions[i];
            
            // create a delete operation for the object
    
            operationPromises[i] = deletion.getStoreOperation();
        }
        
        // return an aggregate promise that waits for all updates to be saved
        // we can safely call Promise.all instead of Promise.settle because
        // Deletion.getStoreOperation has no side-effects - and therefore
        // there's nothing to unwind in case of error
        
        return Promise.all(operationPromises);
    
    })
    
    // wait for all updates to the deleted objects to be saved
    
    .then(function(deleteOperations) {

        // schedule a batch operation to delete the objects in the database

        return Store.issueBatchOperation(client, deleteOperations);
    })
    
    // wait for the deletion to complete
    
    .then(function(result) {
        
        // mark the deletions committed
        
        deletions.forEach(function(deletion) {
            deletion.didCommit();
        });
        
        return deletions;
    });
};

