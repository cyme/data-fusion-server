module.exports = Snapshot;

var Retainable = require('./retainable');
var ServerError = require('./server_error');


// Snapshot inherits from Retainable
// {
//      group:          <Group>,
//      sequence:       #{sequence},
//      updates:        <Map>,
//      retired:        bool
// }

function Snapshot(group)
{
    Retainable.call(this);
    this.group = group;
    this.sequence = -1;
    this.updates = new Map();
    this.retired = false;
}

Snapshot.prototype = Object.create(Retainable.prototype);
Snapshot.constructor = Snapshot;

Snapshot.prototype.register = function()
{
    this.sequence = this.group.currentSequence++;
    this.group.snapshots.push(this);
};

Snapshot.prototype.unregister = function()
{
    var     snapshots;
    var     snapshot;
    var     index;
    var     i;

    this.retired = true;
    snapshots = this.group.snapshots;
    index = snapshots.indexOf(this);
    for(i=0; i<index; i++) {
        if (!snapshots[i].retired)
            break;
    }
    
    if (i < index)
        return;
        
    for(i=0; i<=index; i++) {
        snapshot = snapshots[i];
        snapshot.updates.forEach(function(subclass) {
            subclass.forEach(function(updates) {
                updates.forEach(function(update, key) {
                    update.object.discardEarlierValues(key, snapshot.sequence);
                    update.release();
                });
            });
        });
    }

    snapshots.splice(0, index+1);
};

Snapshot.prototype.addUpdate = function(update)
{
    var     reference;
    var     subclass;
    var     updates;

    reference = update.object.reference;
    subclass = this.updates.get(reference.subclass);
    if (subclass === undefined) {
        subclass = new Map();
        this.updates.set(reference.subclass, subclass);
    }
    updates = subclass.get(reference.globalID);
    if (updates === undefined) {
        updates = new Map();
        subclass.set(reference.globalID, updates);
    }
    update.values.forEach(function(value, key) {
        if (updates.has(key))
            throw new ServerError("Snapshot.addUpdate: multiple update for the same key");
        updates.set(key, update.retain());
    });
};

