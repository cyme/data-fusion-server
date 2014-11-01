module.exports = Reference;

var ServerError = require('./server_error');


// Reference
// {
//      group:      <Group>,
//      type:       "global" | "local",
//      subclass:   "{class name}",
//      globalID:   "{global object id}",
//      localID:    "{local object id}",
//      token:      "{client token}",
//      registered: bool
// }

function Reference(group, type, subclass, id, token)
{
    this.group = group;
    this.type = type;
    this.subclass = subclass;
    this.registered = false;
    switch (type) {
        case Reference.localType:
            this.localID = id;
            this.token = token;
            break;
        case Reference.globalType:
            this.globalID = id;
            break;
        case Reference.nullType:
            break;
        default:
            throw "bad object type";
    }
}

Reference.prototype.register = function(object)
{
    if (this.registered)
        throw new ServerError("Reference.register: inconsistent state");
    this.registered = true;

    if (this.type & Reference.localType)
        this.group.localObjects.set(this.subclass+this.token+this.localID, object); 
    if (this.type & Reference.globalType)
        this.group.globalObjects.set(this.subclass+this.globalID, object);
};

Reference.prototype.unregister = function()
{
    if (!this.registered)
        throw new ServerError("Reference.register: inconsistent state");
    this.registered = false;

    if (this.type & Reference.localType)
        this.group.localObjects.delete(this.subclass+this.token+this.localID); 
    if (this.type & Reference.globalType)
        this.group.globalObjects.delete(this.subclass+this.globalID);
};

Reference.prototype.isRegistered = function()
{
    if (this.type & Reference.localType)
        return this.group.localObjects.has(this.subclass+this.token+this.localID); 
    if (this.type & Reference.globalType)
        return this.group.globalObjects.has(this.subclass+this.globalID);
};

Reference.prototype.getObject = function()
{
    var     object;
    
    if (this.type & Reference.localType)
        object = this.group.localObjects.get(this.subclass+this.token+this.localID);
    if (this.type & Reference.globalType)
        object = this.group.globalObjects.get(this.subclass+this.globalID);
    return (object === undefined ? null : object);
};

Reference.prototype.getValidObject = function()
{
    var     object;
    
    object = this.getObject();
    if ((object === null) || !object.loaded.set || (object.error !== null))
        return null;
    return object;
};

Reference.prototype.validate = function(client, sequence, fix)
{
    var     object;
    
    object = this.getValidObject();
    if ((object === null) || !object.doesExist(sequence)) {
        fix();
        return null;
    }
    return object.retain();
};

Reference.prototype.makeGlobal = function(id, object)
{
    this.type |= Reference.globalType;
    this.globalID = id;
    if (this.registered)
        this.group.globalObjects.set(this.subclass+this.globalID, object);
};

Reference.prototype.isNull = function()
{
    return ((this.type & Reference.nullType) !== 0);
};

Reference.prototype.isGlobal = function()
{
    return ((this.type & Reference.globalType) !== 0);
};

// static function functions

Reference.isReference = function(value)
{
    return (value instanceof Reference);
};


Reference.localType = 1;
Reference.globalType = 2;
Reference.nullType = 4;

Reference.nullReference = new Reference(null, Reference.nullType, null, null);

