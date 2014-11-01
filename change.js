module.exports = Change;

var Retainable = require('./retainable');


// Change inherits from Retainable
// {
//      client:         <Client>,                           /* originating client */
//      object:         <SyncedObject>,
// } 

function Change(client, object)
{
    this.client = client;
    this.object = object;
}

Change.prototype = Object.create(Retainable.prototype);
Change.prototype.constructor = Change;

Change.prototype.register = function()
{
    this.object.retain();
};

Change.prototype.unregister = function()
{
    this.object.release();
};