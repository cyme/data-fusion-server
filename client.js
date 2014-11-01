module.exports = Client;

var Condition = require('sync-utils').Condition;
var Timer = require('sync-utils').Timer;

var Retainable = require('./retainable');
var ServerError = require('./server_error');

// use ES5 and the es6-shim library from Paul Miller until node catches up to a recent enough version
// of V8 (3.27.x or newer) that better supports the ES6 collections

require('es6-shim');


// Client inherits from Retainable
// {
//      group:              <Group>,
//      token:              "<session token>",
//      connection:         <Connection>,
//      timer:              <Timer>,
//      pushed:             <Object>,
//      queries:            <Set>,                  /* list of subscribed queries */
//      workingSet:         <Set>,                  /* list of active client objects */
//      lastPushCompleted:  <Condition>
// }

function Client(group, token)
{
    Retainable.call(this);
    
    this.group = group;
    group.clients.add(this);
    this.token = token;
    this.connection = null;
    this.timer = null;
    this.pushed = {};
    this.queries = new Set();
    this.workingSet = new Set();
    this.lastPushCompleted = Condition.trueCondition;
}

Client.prototype = Object.create(Retainable.prototype);
Client.prototype.constructor = Client;

Client.prototype.register = function()
{
    Client.clients.set(this.token, this);
};

Client.prototype.unregister = function()
{
    console.log("CLOSING SESSION "+this.token);
    
    // clear any timeout

    if (this.timer !== null)
        this.timer.cancel();

    // release all objects in the working set
    
    this.workingSet.forEach(function(count, object) {
        object.release();
    });

    // remove client from query client list
    
    this.queries.forEach(function(query) {
        query.clients.delete(this);
        query.release();
    }, this);
    this.queries.clear();

    // remove client from client registry
    
    this.group.clients.delete(this);

    Client.clients.delete(this.token);
    
    console.log('DUMPING STATE');
    Client.group.queriesByID.forEach(function(query) {
        console.log(' query '+query.id+' ('+query.refcount+')');
    });
    Client.group.globalObjects.forEach(function(object) {
        console.log(' globalObject '+object.reference.globalID+' ('+object.refcount+')');
    });
    Client.group.localObjects.forEach(function(object) {
        console.log(' localObject '+object.reference.localID+' ('+object.refcount+')');
    });

};

Client.prototype.push = function(data)
{
    var     key;
    var     connection;

    connection = this.connection;
    if ((connection !== null) && connection.canPush())
        return this.connection.push(data);
    for(key in data) {
        if (!data.hasOwnProperty(key))
            continue;
        if ((this.pushed[key] === undefined) || !this.pushed.hasOwnProperty(key))
            this.pushed[key] = [];
        this.pushed[key].push(data[key]);
    }
    return;
};

Client.prototype.getPushedData = function()
{
    var     pushed;
    
    pushed = this.pushed;
    this.pushed = {};
    return pushed;
};

Client.prototype.deleteConnection = function()
{
    var     self;

    if (this.connection === null)
        throw new ServerError("Client.deleteConnection: no connection");
    self = this;
    this.connection = null;
    this.timer = new Timer(Client.clientRetentionLag);
    this.timer.wait().then(function() {
        self.release();
    });
};

Client.prototype.setConnection = function(connection)
{
    if (this.connection !== null)
        throw new ServerError("Client.setConnection: existing connection");
    if (this.timer !== null) {
        this.timer.cancel();
        this.timer = null;
    } else {
        this.retain();
    }
    this.connection = connection;
    connection.setClient(this);
};

Client.prototype.addObjectToWorkingSet = function(object)
{
    if (this.workingSet.has(object))
        return false;
    this.workingSet.add(object.retain());
    return true;
};

Client.prototype.removeObjectFromWorkingSet = function(object)
{
    if (!this.workingSet.has(object))
        throw new ServerError("Client.removeObjectFromWorkingSet: unknown object");
    this.workingSet.delete(object);
    object.release();
};

// static functions, variables and constants

Client.clients = new Map();            /* ["{token}", <Client>] pairs */

Client.clientRetentionLag = 60*1000;

