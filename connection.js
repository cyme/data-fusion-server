module.exports = Connection;

var UserError = require('./user_error');
var ServerError = require('./server_error');
var Client = require('./client');


function Connection(protocol, arg)
{
    var     self;
    var     req;
    var     connection;
    var     socket;
    var     token;
    var     client;
    
    function disconnect()
    {
        var     client;
        
        if (protocol == "http")
            Connection.httpConnections.delete(req.socket);
        client = self.client;
        if (client === undefined)
            return;
        client.deleteConnection();
    }
    
    self = this;
    this.protocol = protocol;
    switch(protocol) {
        case "http":
            req = arg;
            connection = Connection.httpConnections.get(req.socket);
            if (connection !== undefined) {
                if ((connection.client !== undefined) && (connection.client.token != req.body.session))
                    throw new UserError("Connection: inconsistent session tokens");
console.log('found existing connection');
                return connection;
            }
            Connection.httpConnections.set(req.socket, this);
            req.socket.on('close', disconnect);
            token = req.body.session;
console.log('creating new connection for ', token);
            if (token !== undefined) {
                client = Client.clients.get(token);
                if (client !== undefined) {
                    if (client.connection !== null)
                        throw new UserError("Connection: establishing multiple connections to the same client");
console.log('reestablishing a connection');
                    client.setConnection(this);
                }
            }
            break;
        case "websocket":
            socket = arg;
            socket.on('disconnect', disconnect);
            break;
        default:
            throw new ServerError("Connection: unknown protocol type");
    }
}

Connection.prototype.canPush = function()
{
    switch(this.protocol) {
        case "http":
            return false;
        case "websocket":
            return true;
    }
};

Connection.prototype.push = function(data)
{
    switch(this.protocol) {
        case "http":
            throw new ServerError("Connection.push: trying to push data on http connection");
        case "websocket":
            this.socket.emit(data);
            break;
    }
    return;
};

Connection.prototype.setClient = function(client)
{
    this.client = client;
    if (this.protocol == "websocket")
        this.push(client.getPushedData());
};

Connection.httpConnections = new Map();
