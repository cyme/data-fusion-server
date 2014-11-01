module.exports = Group;

// use ES5 and the es6-shim library from Paul Miller until node catches up to a recent enough version
// of V8 (3.27.x or newer) that better supports the ES6 collections

require('es6-shim');

var     Reference = require('./reference');
var     ServerError = require('./server_error');


// Group
// {
//      clients:            <Set>,      /* list of clients */
//      globalObjects:      <Map>,      /* ["{global id}", <SyncedObject>] pairs */
//      localObjects:       <Map>,      /* ["{local id}", <SyncedObject>] pairs */
//      snapshots:          <Array>,    /* [ <Snapshot> ... ] */
//      currentSequence:    #{sequence},
//      queriesByID:        <Map>,      /* ["{query id}", <Query>] pairs */
//      queriesBySubclass:  <Map>,      /* ["{class name}", <Query>] pairs */
//      pendingCreations:   <Map>,      /* ["{class name"}, <Set>] pairs */
//      pendingDeletions:   <Map>,      /* ["{class name"}, <Set>] pairs */
//      pendingUpdates:     <Map>       /* ["{class name"}, <Set>] pairs */
//      referenceUpdates:   <Map>       /* ["{class name}{object id}", <Map> of [<SyncedObject>, <Set> of "{key}"] pairs ] pairs */
// }

function Group()
{
    this.clients = new Set();
    this.globalObjects = new Map();
    this.localObjects = new Map();
    this.snapshots = [];
    this.currentSequence = 1;
    this.queriesByID = new Map();
    this.queriesBySubclass = new Map();
    this.pendingCreations = new Map();
    this.pendingDeletions = new Map();
    this.pendingUpdates = new Map();
    this.referenceUpdates = new Map();
}

Group.prototype.needReferenceUpdate = function(reference, object, key)
{
    var     entry;
    var     references;
    var     updates;

console.log('NeedReferenceUpdate for ref '+reference.globalID+' at '+object.reference.globalID+'/'+key);
    if (!reference.isGlobal())
        throw new ServerError("Group.needReferenceUpdate: reference isn't global");

    entry = reference.subclass + reference.globalID;
    references = this.referenceUpdates.get(entry);
    if (references === undefined) {
        references = new Map();
        this.referenceUpdates.set(entry, references);
    }

    updates = references.get(object);
    if (updates === undefined) {
        updates = new Set();
        references.set(object, updates);
    }
    updates.add(key);
};

Group.prototype.dropReferenceUpdate = function(reference, object, key)
{
    var     entry;
    var     references;
    var     updates;

console.log('DropReferenceUpdate for ref '+reference.globalID+' at '+object.reference.globalID+'/'+key);
    if (!reference.isGlobal())
        throw new ServerError("Group.dropReferenceUpdate: reference isn't global");
    entry = reference.subclass + reference.globalID;
    references = this.referenceUpdates.get(entry);
    if (references === undefined)
        return false;

    updates = references.get(object);
    if (updates === undefined)
        return false;
    if (!updates.has(key))
        return false;
        
    updates.delete(key);
    if (updates.size === 0) {
        references.delete(object);
        if (references.size === 0)
        this.referenceUpdates.delete(entry);
    }
    return true;
};

Group.prototype.updateReference = function(relation)
{
    var     reference;
    var     entry;
    var     references;

console.log('UpdateReference for ref '+relation.reference.globalID);
    reference = relation.reference;
    entry = reference.subclass + reference.globalID;
    references = this.referenceUpdates.get(entry);
    if (references === undefined)
        return;
    references.forEach(function(updates, object) {
        updates.forEach(function(key) {
            relation.addReference(object, key);
        });
        references.delete(object);
    });
    this.referenceUpdates.delete(entry);
};


// static functions, variables and constants

Group.group = new Group();