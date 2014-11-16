module.exports = Request;

var JSONSafeParse = require('json-safe-parse');

var Condition = require('sync-utils').Condition;

var SyncError = require('./sync_error');
var ServerError = require('./server_error');
var UserError = require('./user_error');
var Reference = require('./reference');
var SyncedObject = require('./synced_object');
var Creation = require('./creation');
var Deletion = require('./deletion');
var Update = require('./update');
var Connection = require('./connection');



function Request(connection)
{
    var     req;
    var     res;
    var     data;
    var     responder;

    this.connection = connection;
    switch(connection.protocol) {
        case "http":
console.log('creating new request');
            req = arguments[1];
            res = arguments[2];
            this.req = req;
            this.res = res;
console.log('done');
            break;
        case "websocket":
            data = arguments[1];
            responder = arguments[2];
            if (data === undefined)
                throw new ServerError("Request: no data");
            if (data === undefined)
                throw new ServerError("Request: no responder");
            this.data = data;
            this.responder = responder;
            break;
    }
}

Request.prototype.getData = function()
{
    switch(this.connection.protocol) {
        case "http":
            JSONSafeParse.cleanse(this.req.body);
            return this.req.body;
        case "websocket":
            JSONSafeParse.cleanse(this.data);
            return this.data;
    }
};

Request.prototype.getToken = function()
{
    switch(this.connection.protocol) {
        case "http":
            return this.req.body.session;
        case "websocket":
            return this.data.token;
    }
};

Request.prototype.respond = function(data)
{
    var     client;
    var     merged;
    var     key;
    
    client = this.connection.client;
    if (client !== undefined) {
        merged = client.getPushedData();
        for(key in data) {
            if (!data.hasOwnProperty(key))
                continue;
            if ((merged[key] === undefined) || !merged.hasOwnProperty(key)) {
                if (Array.isArray(data[key]))
                    merged[key] = data[key];
                else
                    merged[key] = [ data[key] ];
            } else {
                if (Array.isArray(data[key])) {
                    data[key].forEach(function(item) {
                        merged[key].push(item);
                    });
                } else
                    merged[key].push(data[key]);
            }
        }
    } else
        merged = data;

    switch(this.connection.protocol) {
        case "http":
            this.res.status(200);
            this.res.send(merged);
            return;
        case "websocket":
            if (this.responder === undefined)
                throw new ServerError("Request.respond: no responder set");
            this.responder(merged);
            return;
    }
};

Request.prototype.error = function(error)
{
    if (!SyncError.isError(error))
        error = new ServerError(error);
    switch(this.connection.protocol) {
        case "http":
            if (error.isInternal())
                this.res.status(500);
            else
                this.res.status(409);
            this.res.send({ error: error.getReason() });
            break;
        case "websocket":
            this.responder({ error: error.getReason() });
            break;
    }
};

Request.handleHTTPRequest = function(req, res, handler)
{
    try {
        var     connection;
        var     request;
    
        connection = new Connection("http", req);
        request = new Request(connection, req, res);
        handler(request);
        
    } catch (exception) {
        
        var     error;
        
        if (!SyncError.isError(exception))
            error = new ServerError(exception);
        else
            error = exception;
        if (error.isInternal())
            res.status(500);
        else
            res.status(409);
        res.send({ error: error.getReason() });
    }
};

// Request.parseGlobalReference = function(client, specs)
// safely parses a client global reference and returns it
// synchronous

Request.parseGlobalReference = function(client, specs)
{
    if ((typeof specs.subclass !== "string") || (typeof specs.id !== "string"))
        throw new UserError("Request.parseGlobalReference: malformed request");
    return new Reference(client.group, Reference.globalType, specs.subclass, specs.id);
};


// Request.parseLocalReference = function(client, specs)
// safely parses a client local reference and returns it
// synchronous

Request.parseLocalReference = function(client, specs)
{
    if ((typeof specs.subclass !== "string") || (typeof specs.id !== "string"))
        throw new UserError("Request.parseLocalReference: malformed request");
    return new Reference(client.group, Reference.localType, specs.subclass, specs.id, client.token);
};

// Request.parseValues = function(client, specs)
// safely parse client object values and return them as a <Map> of [ "{key"}, {value} ] pairs
// synchronous

Request.parseValues = function(client, specs)
{
    var     values;
    
    if (!Array.isArray(specs.values))
        throw new UserError("Request.parseValues: malformed request");

    values = new Map();
    specs.values.forEach(function(pair) {
        var     key;
        var     description;
        var     value;
        
        if (!Array.isArray(pair) || (pair.length !== 2))
            throw new UserError("Request.parseValues: malformed request");
        key = pair[0];
        description = pair[1];
        if (typeof key != "string")
            throw new UserError("Request.parseValues: malformed request");
        switch(typeof description) {
            case "object":
                if (typeof description.type !== "string")
                    throw new UserError("Request.parseValues: wrong pointer type");
                if ((typeof description.subclass !== "string") || (typeof description.id !== "string"))
                    throw new UserError("Request.parseValues: wrong pointer type");
                switch(description.type) {
                    case "global":
                        value = new Reference(client.group, Reference.globalType, description.subclass, description.id);
                        break;
                    case "local":
                        value = new Reference(client.group, Reference.localType, description.subclass, description.id, client.token);
                        break;
                    case "null":
                        value = Reference.nullReference;
                        break;
                    default:
                        throw new UserError("Request.parseValues: wrong pointer type");
                }
                break;
            case "number":
            case "string":
                value = description;
                break;
            default:
                throw new UserError("Request.parseValues: malformed request");
        }
        if (values.has(key))
            throw new UserError("Request.parseValues: malformed request");
        values.set(key, value);
     });
     return values;
};



// Request.parseCreations = function(client, body)
// extract the list of object created by the client from the client request
// returns a promise that resolves to an array of <Creations>
// side-effect: the <Creations> returned retain their object
//
//      [...]
//      "creations": [
//          {
//              "subclass": "{class name}",
//              "id": "{local id}",
//              "values": [
//                  [ "{property name}", "{string value}" | #{number value} ]
//                  [
//                      "{relation name}",
//                      {
//                          "type": "global",
//                          "subclass": "{class name}",
//                          "id": "{object id}"
//                      }
//                  ]
//                  [
//                      "{relation name}",
//                      {
//                          "type": "local",
//                          "subclass": "{class name}",
//                          "id": "{local id}"
//                      }
//                  ]
//                  ...
//              ]
//          }
//      ]

Request.parseCreations = function(client, body)
{
    var     creations;
    var     objects;

    if (!body.creations)
        return Promise.resolve([]);
    
    // the code is executed within a Promise.evaluate so that any throw is turned into a rejection

    return Promise.evaluate(function() {
        
        var     group;
        var     specs;
        var     reference;
        var     values;
        var     object;
        var     creation;
        var     i;

        if (!Array.isArray(body.creations))
            throw new UserError("Request.parseCreations: malformed request");

        /*
        Identify the list of new objects in the parsed request.
        We proceed in 2 steps. We create the objects without data first. Then we populate the
        object data. This allows correct handling of references to new objects.
        */
    
        group = client.group;

        objects = new Array(body.creations.length);
        creations = new Array(body.creations.length);

        for(i=0; i<body.creations.length; i++) {
            
            specs = body.creations[i];
            if (typeof specs !== "object")
                throw new UserError("Request.parseCreations: malformed request");

            reference = Request.parseLocalReference(client, specs);
            if (reference.isRegistered())
                throw new UserError("Request.parseCreations: object id already in use");
            objects[i] = new SyncedObject(group, reference, new Condition(), Condition.trueCondition, true).retain();
        }

        // second step: we populate the object values
        
        for(i=0; i<body.creations.length; i++) {
            
            specs = body.creations[i];
            object = objects[i];
            values = Request.parseValues(client, specs);
            
            // we don't have a snapshot yet. we use 0 as the sequence for the initial
            // object values. we guarantee that reading object values with a
            // sequence that predate its creation sees undefined values because:
            // 1) the object won't be registered until the creation is actuated
            // 2) any access to the object value after the creation is actuated checks the
            //      creation sequence

            values.forEach(function(value, key) {
                object.setValue(key, value, 0);
            });
        }

        // create an array of creations from the objects and return it
        
        for(i=0; i<body.creations.length; i++) {
            creation = new Creation(client, objects[i]);
            creations[i] = creation.retain();
            
            // balance the retain issued at the object instantiation
            
            objects[i].release();
        }
        
        return creations;
    })
    
    // if any error is encountered along the way, deal with it here
    
    .catch(function(error) {
        
        var     i;
        
        for(i=0; i<body.creations.length; i++)
            if (objects[i] !== undefined)
                objects[i].release();
        return Promise.reject(error);
    });
};


// Request.parseDeletions = function(client, body)
// extract the list of object deleted by the client from the client request
// returns a promise that resolves to an array of <Deletions>
// side-effect: the <Deletions> returned retain their object
//
//      [...]
//      "deletions": [
//          {
//              "subclass": "{class name}",
//              "id": "{object id}",
//          }
//          ...
//      ]

Request.parseDeletions = function(client, body)
{
    var     references;
    var     objectPromises;
    var     objects;
    var     deletions;

    if (body.deletions === undefined)
        return Promise.resolve([]);

    // the code is executed within a Promise.evaluate so that any throw is turned into a rejection
    
    return Promise.evaluate(function() {
 
        var     specs;
        var     i;


        references = new Array(body.deletions.length);
    
        // Identify the list of deleted objects in the parsed request
    
        for(i=0; i<body.deletions.length; i++) {
    
            specs = body.deletions[i];
            
            // do some sanity check on the request body
            
            if (typeof specs.subclass !== "string")
                throw new UserError("Request.parseDeletions: malformed request (missing or invalid subclass)");
            if (typeof specs.id !== "string")
                throw new UserError("Request.parseDeletions: malformed request (missing or invalid object id)");
                
            // extract the reference to the deleted object
            
            references[i] = new Reference(client.group, Reference.globalType, specs.subclass, specs.id);
        }
    
        // load the deleted objects if they are not loaded already

        objectPromises = new Array(body.deletions.length);

        for(i=0; i<body.deletions.length; i++) {
            objectPromises[i] = SyncedObject.load(client, references[i], 0);
        }

        // wait for all deleted objects to succesfully load
    
        return Promise.settle(objectPromises);
        
    })
    
    // then create deletions for all those objects
    
    .then(function(results) {
        
        var     object;
        var     deletion;
        var     i;
        
        objects = results;
        deletions = new Array(objects.length);
        for(i=0; i<objects.length; i++) {
            object = objects[i];
            
            deletion = new Deletion(client, object);
            deletions[i] = deletion.retain();
            
            // release the object to balance out the retain in SyncedObject.load
            
            object.release();
        }
        return deletions;
    })
    
    // if any error was encountered along the way, deal with it here
    
    .catch(function(reason) {
        
        var     i;
        var     objects;
        var     rejected;
console.log('deletions failed');


        // did we fail while creating the deletion objects?
        
        if (deletions !== undefined) {
            
            // drop the deletion objects that have been created
            
            for(i=0; i<deletions.length; i++) {
                if (deletions[i] !== undefined)
                    deletions[i].drop();
            }
            
            // release all the objects that were loaded
            
            for(i=0; objects.length; i++) {
                objects[i].release();
            }
            
            // propagate the rejection
            
            return Promise.reject(reason);
        }
        
        // did we fail while loading the objects?
        
        if (objectPromises !== undefined) {
            
            // the failure reason is returned by Promise.settle
            // it contains an array of resolved promises (loaded objects) and
            // an array of rejection reasons
            
            // let release the objects that have been loaded
            
            objects = reason.resolved;
            for(i=0; i<objects.length; i++) {
                if (objects[i] !== undefined)
                    objects[i].release();
            }
            
            // let return a rejection with the first reason we find
            
            rejected = reason.rejected;
            for(i=0; i<rejected.length; i++) {
                if (rejected[i] !== undefined)
                    return Promise.reject(rejected[i]);
            }
            
            // we should not get here
            
            return Promise.reject(new ServerError("Request.parseDeletions: internal error"));
        }
        
        // we failed while parsing the body. simply propagate the reason
        
        return Promise.reject(reason);
    });
};

// Update.getClientUpdates(client, body)
// extract the list of client updates from the client request
// returns a promise that resolves to an array of <Updates>
// side-effect: the <Updates> returned retain their object
//
//      [...]
//      "updates": [
//          {
//              "subclass": "{class name}",
//              "id": "{object id}",
//              "version": #{object base version},
//              "values": [
//                  [ "{property name}", "{string value}" | #{number value} ]
//                  [
//                      "{relation name}",
//                      {
//                          "type": "global",
//                          "subclass": "{class name}",
//                          "id": "{object id}"
//                      }
//                  ]
//                  [
//                      "{relation name}",
//                      {
//                          "type": "local",
//                          "subclass": "{class name}",
//                          "id": "{local id}"
//                      }
//                  ]
//                  ...
//              ]
//          }
//      ]

Request.parseUpdates = function(client, body)
{
    var     references;
    var     valuess;
    var     versions;
    var     objectPromises;
    var     objects;
    var     updates;

    if (body.updates === undefined)
        return Promise.resolve([]);

    // the code is executed within a Promise.evaluate so that throws are turned into rejections

    return Promise.evaluate(function() {

        var     size;
        var     reference;
        var     values;
        var     version;
        var     specs;
        var     i;

        if (!Array.isArray(body.updates))
            throw new UserError("Request.parseUpdates: malformed request");
        
        size = body.updates.length;
        references = new Array(size);
        valuess = new Array(size);
        versions = new Array(size);
    
        for(i=0; i<size; i++) {
            specs = body.updates[i];
            
            if (typeof specs !== "object")
                throw new UserError("Request.parseUpdates: malformed request");
            if (typeof specs.version !== "number")
                throw new UserError("Request.parseUpdates: malformed request");

            version = specs.version;
            reference = Request.parseGlobalReference(client, specs);
            values = Request.parseValues(client, specs);

            references[i] = reference;

            valuess[i] = values;
            versions[i] = version;
        }
    })
    
    // deal with errors encountered in the previous section
    
    .catch(function(error) {
        
        return Promise.reject(error);
    })
    
    // now load the objects referenced in the updates
    
    .then(function() {

        var         i;
        
        objectPromises = new Array(body.updates.length);
        for(i=0; i<body.updates.length; i++) {
            objectPromises[i] = SyncedObject.load(client, references[i], 0);
        }

        // wait for all objects to succesfully load

        return Promise.settle(objectPromises);
    })

    // deal with errors encountered while loading the objects
    
    .catch(function(reason) {
        
        var     objects;
        var     rejected;
        var     i;
        
        // the failure reason is returned by Promise.settle
        // it contains an array of resolved promises (loaded objects) and
        // an array of rejection reasons
        
        // let release the objects that have been loaded
        
        objects = reason.resolved;
        for(i=0; i<objects.length; i++) {
            if (objects[i] !== undefined)
                objects[i].release();
        }
        
        // let return a rejection with the first reason we find
        
        rejected = reason.rejected;
        for(i=0; i<rejected.length; i++) {
            if (rejected[i] !== undefined)
                return Promise.reject(rejected[i]);
        }
        
        // we should not get here, but we will if all promises rejected with undefined reason
        
        return Promise.reject(undefined);
    })
    
    // then create an array of updates from those objects and return it
    
    .then(function(results) {
        
        var     object;
        var     update;
        var     i;
        
        objects = results;

        // ensure there is no more than one update per object
        
        if (objects.length !== new Set(objects).size)
            throw new UserError("Request.parseUpdates: malformed request");

        updates = new Array(objects.length);
        for(i=0; i<objects.length; i++) {
            object = objects[i];
            update = new Update(client, object, versions[i], valuess[i]);

            updates[i] = update.retain();

            // release the object to balance out the retain in SyncedObject.load

            object.release();
        }

        return updates;
    })
    
    // if any error was encountered in the last section, deal with it here
    
    .catch(function(reason) {
        var     i;
        
        for(i=0; i<updates.length; i++) {
            if (updates[i] !== undefined)
                updates[i].release();
        }
        return Promise.reject(reason);
    });
};


// Request.composeObjectList = function(objects, sequence)
// create a well-formatted list readable by clients that captures a list of object
// synchronous

Request.composeObjectList = function(objects, sequence)
{
    var     list;
    
    list = [];
    objects.forEach(function(object) {
        list.push({
                subclass: object.reference.subclass,
                id: object.reference.globalID,
                version: object.version,
                values: Request.composeObjectValues(object, sequence)
        });
    });
    return list;
};


// Request.composeObjectValues = function(object, sequence)
// create a well-formatted object readable by clients that captures an object values
// synchronous

Request.composeObjectValues = function(object, sequence)
{
    var     values;
    
    values = [];
    object.forEachValue(sequence, function(value, key) {
        values.push([key, Request.composeValue(value)]);
    });
    return values;
};


// Request.composeUpdateValues = function(update)
// create a well-formatted object readable by clients that captures an update values
// synchronous

Request.composeUpdateValues = function(update)
{
    var     values;
    
    values = [];
    update.values.forEach(function(value, key) {
        values.push([key, Request.composeValue(value)]);
    });
    return values;
};


// Request.composeValue = function(value)
// create a well-formatted object or primitive value readable by clients that
// captures a value
// synchronous

Request.composeValue = function(value)
{
    var     reference;
    
    if (Reference.isReference(value)) {
        reference = value;
        if (reference.isNull())
            return {
                type: "null"
            };
        if (!reference.isGlobal())
            throw new ServerError("createClientValue: reference is not global");
        return {
            type: "global",
            subclass: value.subclass,
            id: value.globalID
        };
    }
    return value;
};


// Request.composeQueryList = function(queries)
// returns a well-formatted array of query ids that reflect a <Set> of queries
// synchronous

Request.composeQueryList = function(queries)
{
    var     result;
    
    result = [];
    queries.forEach(function(query) {
        result.push(query.id);
    });
    return result;
};

