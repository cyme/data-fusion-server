module.exports = Creation;

var Change = require('./change');
var ServerError = require('./server_error');
var Store = require('./store');

// use ES5 and the es6-shim library from Paul Miller until node catches up to a recent enough version
// of V8 (3.27.x or newer) that better supports the ES6 collections

require('es6-shim');



// Creation inherits from Change
// {
//      fixed:      bool,                   /* client submitted dangling references */
//      fixedKeys:  <Array>,                /* list of keys to dangling references */
//      qualifying: <Set>                   /* list of all qualifying queries */
// }

function Creation(client, object)
{
    Change.call(this, client, object);
    
    this.fixed = false;
    this.fixedKeys = [];
    this.qualifying = new Set();
}

Creation.prototype = Object.create(Change.prototype);
Creation.prototype.constructor = Creation;

Creation.prototype.forEachValue = function(fn)
{
    this.object.forEachValue(this.object.creationSequence, fn);
};

Creation.prototype.forEachRelation = function(fn)
{
    this.object.forEachRelation(this.object.creationSequence, fn);
};

Creation.prototype.actuate = function(snapshot)
{
    var     sequence;
    var     object;
    var     group;
    var     subclass;

    object = this.object;
    sequence = snapshot.sequence;
    if (object.creationSequence !== -1)
        throw new ServerError("Creation.actuate: already actuated");
    object.creationSequence = sequence;
    object.version = 1;
    this.forEachValue(function(key, value) {
        object.updateVersions.set(key, 1);
    });
    
    group = object.group;
    subclass = group.pendingCreations.get(object.reference.subclass);
    if (subclass === undefined) {
        subclass = new Map();
        group.pendingCreations.set(object.reference.subclass, subclass);
    }
    subclass.set(object.reference.localID, this.retain());
};

Creation.prototype.didCommit = function()
{
    var     object;
    var     group;
    var     subclass;

    object = this.object;
    group = object.group;

    subclass = group.pendingCreations.get(object.reference.subclass);
    subclass.delete(object.reference.localID);
    this.release();
    
    object.created.signal();
};

Creation.prototype.qualify = function(query)
{
    this.qualifying.add(query);
    query.qualified.add(this.object.retain());
};

// returns a create operation, which instructs to commit the object to the store

Creation.prototype.getStoreOperation = function()
{
    return {
        type: "CREATE",
        object: this.object,
    };
};

// returns a patch operation, which instructs to patch a relation

Creation.prototype.getPatchStoreOperation = function(key)
{
    var         object;
    
    object = this.object;
    return {
        type: "UPDATE",
        update: null,
        object: object,
        version: object.version,
        values: new Map([[key, object.getValue(key, object.creationSequence)]])
    };
};

Creation.prototype.toString = function()
{
    return 'Creation { class: '+this.object.reference.subclass+', id: '+this.object.reference.localID+' }';
};




// static functions, variables and constants


// Creation.commitToStore = function(client, creations)
// asynchronously create in the store the objects pointed by the array of <Creation> passed
// returns a promise that resolves to the creations
// executes without side-effects: caller doesn't need to unwind when handling errors

Creation.commitToStore = function(client, creations)
{
    var     oneStepCreations;
    var     twoStepCreations;
    var     refersToCreations;
    var     creationOperations;
    var     patchOperations;

    return Promise.evaluate(function() {
        
        /*    
        objects that make references to new objects must be saved in 2 steps:
        the object is first created without the references to the new objects. Then the
        references to the new objects are patched after all new objects have been created.
        this properly handles the case of circular references.
        */
        
        creationOperations = [];
        patchOperations = [];
        oneStepCreations = [];
        twoStepCreations = [];
        
        creations.forEach(function(creation) {
            
            var     object;
            var     sequence;
            
            /*
            here we check if the created object makes references to new objects
            if it does, we remember the references and set them to null before saving the object
            the references will be saved later
            */
            
            object = creation.object;
            sequence = object.creationSequence;
            refersToCreations = false;
            
            // iterate over all object relations
            
            object.forEachRelation(sequence, function(reference, key) {
                
                // check if the relation points to a local (new) object
                
                if (!reference.isGlobal()) {
                    
                    // it is a new object. remember to patch the reference
                    
                    patchOperations.push(creation.getPatchStoreOperation(key));
    
                    refersToCreations = true;
                }
            });
            
            // if the new object makes no reference to new objects, it'll be saved in one step.
    
            if (!refersToCreations)
                oneStepCreations.push(creation);
            else
                twoStepCreations.push(creation);
                    
            // remember to create the object
                    
            creationOperations.push(creation.getStoreOperation());
        });
        
        /*
        asynchronously save all the new objects.
        when all the saves have completed, we retrieve the object ids.
        we notify the object creation is complete for those that make no reference to new objects.
        then we asynchronously save the references to new objects.
        when the references are saved, we notify the object creation is complete for the
        remaining objects.
        */
    
        return Store.issueBatchOperation(client, creationOperations);
    
    })
    
    // wait for the object creation to complete
    
    .then(function(results) {
        
        // retrieve the object ids
        
        results.forEach(function(result) {
            var     object;
            
            object = result.operation.object;
            object.reference.makeGlobal(result.id, object);
        });

        // we can now mark the 1-step creations committed
        
        oneStepCreations.forEach(function(creation) {
            creation.didCommit();
        });

        // we can patch the references to new objects now
        
        return Store.issueBatchOperation(client, patchOperations);
    })
    
    // wait for the patch operations to complete
    
    .then(function(results) {
        
        // we can now mark the 2-step creations committed
        
        twoStepCreations.forEach(function(creation) {
            creation.didCommit();
        });

        return creations;
    });
};
