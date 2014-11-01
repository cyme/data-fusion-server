module.exports = Parse;

var https = require('https');

var Reference = require('./reference');
var ServerError = require('./server_error');


// Parse utility functions

function Parse()
{
}

Parse.relation = function(reference)
{
    // null references cause the key holding them to be deleted
    // references to local (unsaved) objects are saved as null references.

    if (!reference.isGlobal())
        return {
            __op: "Delete"
        };
        
    return {
        __type: "Pointer",
        className: reference.subclass,
        objectId: reference.globalID
    };
};

// Parse.issueHTTPSRequest(sessionToken, reqMethod, reqPath, reqDataURLencode, reqDataBody)
// asynchronously make a HTTPS request to the Parse server using the arguments provided
// returns a promise that resolves to the result of the request
// executes without side-effects: it doesn't need to be unwound when handling errors

Parse.issueHTTPSRequest = function(sessionToken, reqMethod, reqPath, reqDataURLencode, reqDataBody)
{
    return new Promise(function(fulfill, reject) {
        var     options;
        var     headers;
        var     encodedPath;
        var     req;

        headers = {
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'X-Parse-Application-Id': Parse.applicationID,
            'X-Parse-REST-API-Key': Parse.RESTAPIKey,
            'X-Parse-Session-Token': sessionToken
        };
        encodedPath = reqPath;
        if ((reqDataURLencode !== undefined) && (reqDataURLencode !== ""))
            encodedPath += '?'+encodeURIComponent(reqDataURLencode);
        options = {
            hostname: 'api.parse.com',
            port: 443,
            headers: headers,
            path: encodedPath,
            method: reqMethod
        };
        req = https.request(options);
        req.on('error', function(err) {
            console.log('HTTPSRequest error');
            console.log(err);
            reject(err);
        });
        req.on('response', function(res) {
            var     data;
            
            data = '';
            res.on('data', function(chunk) {
                data += chunk;
            });
            res.on('end', function() {
                
                // should protect against malformed responses
                fulfill({
                    status: res.statusCode,
                    body: JSON.parse(data)
                });
            });
        });
        req.end(reqDataBody);
    });
};

// Parse.getRequest(operation, batched)
// create a parse request object given an operation
// synchronous

Parse.getRequest = function(operation, batched)
{
    var     object;
    var     sequence;
    var     body;

    // create a formatted request
    
    switch(operation.type) {
        case "LOAD":
            object = operation.object;
            if (batched)
                throw new ServerError("Parse.getRequest: batch LOAD operations not supported");
            return {
                method: "GET",
                path: "/1/classes/"+object.reference.subclass+"/"+object.reference.globalID
            };
        case "CREATE":
            object = operation.object;
            sequence = object.creationSequence;
            body = {};
            object.forEachValue(function(value, key, isRelation) {
                body[key] = isRelation ? Parse.relation(value) : value;
            });

            return {
                method: "POST",
                path: "/1/classes/"+object.reference.subclass,
                body: body
            };
        case "DELETE":
            object = operation.object;
            return {
                method: "DELETE",
                path: "/1/classes/"+object.reference.subclass+"/"+object.reference.globalID
            };
        case "UPDATE":
            object = operation.object;
            body = {};
            operation.values.forEach(function(value, key) {
                if (Reference.isReference(value))
                    body[key] = Parse.relation(value);
                else
                    body[key] = value;
                body[Parse.keyVersionPrefix+key] = operation.version;
            });
            var res = {
                method: "PUT",
                path: "/1/classes/"+object.reference.subclass+"/"+object.reference.globalID,
                body: body
            };
console.log(res);
            return res;
        case "QUERY":
            return {
                method: "GET",
                path: "/1/classes/"+operation.subclass
            };
        default:
            throw new ServerError("Parse.getRequest: invalid operation");
    }
};

// Parse.extractObject(client, body, object)
// extract the content of the object data obtained from Parse
// synchronous

Parse.extractObject = function(client, body, object)
{
    var     key;
    var     relation;
    var     value;
    var     version;
    var     lastVersion;
    
    lastVersion = 0;

    for(key in body) {

        if (!body.hasOwnProperty(key))
            continue;
        if (Parse.privateKeys.indexOf(key) != -1)
            continue;
            
        // skip if this is a key version
        
        if (key.indexOf(Parse.keyVersionPrefix) === 0)
            continue;

        switch (typeof body[key]) {
            
            // is this a property?
            
            case "string":
            case "number":
                value = body[key];
                break;
                
            // is this a relation?
            
            case "object":
                relation = body[key];
                if ((relation.__type !== "Pointer") || (typeof relation.className !== "string") || (typeof relation.objectId !== "string"))
                    throw new ServerError("Parse.extractObject: malformed LOAD data from Parse (bad pointer)");
                value = new Reference(client.group, Reference.globalType, relation.className, relation.objectId);
                break;
                
            default:
                throw new ServerError("Parse.extractObject: malformed LOAD data from Parse (bad type)");
        }

        // look for the corresponding version
        
        if (body[Parse.keyVersionPrefix+key] !== undefined)
            version = parseInt(body[Parse.keyVersionPrefix+key], 10);
        else {
            if (Parse.lenientWithVersions)
                version = 1;
            else
                version = NaN;
        }
            
        if (isNaN(version))
            throw new ServerError("Parse.extractObject: invalid version number");

        object.setValue(key, value, 0);
        object.updateVersions.set(key, version);
        
        if (version > lastVersion)
            lastVersion = version;
    }
    
    // second pass to identify deleted keys
    
    for(key in body) {

        if (!body.hasOwnProperty(key))
            continue;
        if (Parse.privateKeys.indexOf(key) != -1)
            continue;
            
        // skip if this is not a key version
        
        if (key.indexOf(Parse.keyVersionPrefix) !== 0)
            continue;
        
        // skip if this key was already found in the first pass
        
        key = key.substr(Parse.keyVersionPrefix.length);
        if (body[key] !== undefined)
            continue;
        
        // it's a deleted key. remember it as a null reference
        
        version = parseInt(body[Parse.keyVersionPrefix+key], 10);
        value = Reference.nullReference;
        object.setValue(key, value, 0);
        object.updateVersions.set(key, version);
        
        if (version > lastVersion)
            lastVersion = version;
    }
    
    object.version = lastVersion;
};

Parse.applicationID = 'JNaR21WONgQHZNSCbsmkmdQxW3tydY6pVfprNt6V';
Parse.RESTAPIKey = 'lZd4lvVg6S9Cn8t0EjFbOGpXPJM3GwaWvaRZLqgO';
Parse.privateKeys = ["objectId", "createdAt", "updatedAt", "ACL"];
Parse.keyVersionPrefix = "KEYVERSION";
Parse.lenientWithVersions = true;
