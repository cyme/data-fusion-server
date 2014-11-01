module.exports = Store;

var Condition = require('sync-utils').Condition;

var Parse = require('./parse');
var SyncError = require('./sync_error');
var UserError = require('./user_error');
var Reference = require('./reference');
var SyncedObject = require('./synced_object');
var ServerError = require('./server_error');


// use ES5 and the es6-shim library from Paul Miller until node catches up to a recent enough version
// of V8 (3.27.x or newer) that better supports the ES6 collections

require('es6-shim');


function Store()
{
}

// Store.issueOperation(client, operation)
// initiate a single Parse operation
// returns a promise that returns the request result

Store.issueOperation = function(client, operation)
{
    var     request;

    return Promise.evaluate(function() {
        
        request = Parse.getRequest(operation, false);
    
        // initiate the request
        
        return Parse.issueHTTPSRequest(client.token, request.method, request.path, request.urlEncode, request.body);
    })
    
    // wait until the request completes
    
    .then(function(parseResult) {

        var     result;
        var     body;
        var     promises;

        // check for error
        
        if ((parseResult.status < 200) || (parseResult.status > 299)) {
            if (parseResult.body.code == 101)
                throw new UserError(SyncError.errorObjectNotFound);
            throw new UserError(parseResult);
        }
        
        // read the results

        result = {};
        result.operation = operation;
        body = parseResult.body;
        switch(operation.type) {
            
            // for CREATE: get the object id

            case "CREATE":
                result.id = body.objectId;
                return result;

            // for LOAD: populate the passed object and return it UNRETAINED

            case "LOAD":
                Parse.extractObject(client, body, operation.object);
                result.object = operation.object;
                return result;

            // for DELETE and UPDATE: nothing more to do
            
            case "DELETE":
            case "UPDATE":
                return result;

            // for QUERY: instantiate all the objects matching the query and return
            // a list of RETAINED objects
            
            case "QUERY":
                result.objects = new Set();
                promises = [];
                body.results.forEach(function(parseObject) {
                    var     reference;
                    var     object;
                    var     promise;

                    reference = new Reference(client.group, Reference.globalType, operation.subclass, parseObject.objectId);
                    object = reference.getObject();
                    
                    // if this object isn't instantiated yet, instantiate and add it to the results
                    // if it is instantiated, wait until it is loaded and add it to the results if it was successfully loaded

                    if (object === null) {
                        object = new SyncedObject(client.group, reference, Condition.trueCondition, Condition.trueCondition, false);
                        Parse.extractObject(client, parseObject, object);
                        result.objects.add(object.retain());
                    } else {
                        object.retain();
                        promise = object.loaded.wait().then(function(){
                            if (object.error === null)
                                result.objects.add(object);
                        });
                        promises.push(promise);
                    }
                });
                return Promise.all(promises).then(function() {
                    return result;
                });
            default:
                throw new ServerError('Store.issueOperation: bad operation type');
        }
    });
};


// Store.issueBatchStoreOperation(client, operations)
// initiate multiple Parse operations
// returns a promise that returns the request results in an array

Store.issueBatchOperation = function(client, operations)
{
    var     buffer;
    var     allBuffers;
    var     promises;
    var     promise;
    var     operation;        
    var     request;
    var     body;
    var     i;

    return Promise.evaluate(function() {
        
        // Parse supports batches of up to 50 operations
    
        buffer = [];
        allBuffers = [buffer];
        promises = [];
        
        // iterate over all operations

        for(i=0; i<operations.length; i++) {
            
            operation = operations[i];
            
            // create a new buffer if we have exceed the batch limit (50)
            
            if (buffer.length >= 50) {
                buffer = [];
                allBuffers.push(buffer);
            }
            
            // create a formatted request
            
            request = Parse.getRequest(operation, true);
            buffer.push(request);
        }

        // initiate the batch requests
        
        for(i=0; i<allBuffers.length; i++) {
    
            buffer = allBuffers[i];
            body = JSON.stringify({
                requests: buffer
            });
            
            promise = Parse.issueHTTPSRequest(client.token, "POST", "/1/batch", "", body);
            promises.push(promise);
        }
        
        // construct an aggregate promise that returns the request results
        // note that we can safely use Promise.all instead of Promise.settle here
        // because we don't need to unwind anything in case of error

        return Promise.all(promises);
    
    })
    
    // wait until all batch requests have completed
    
    .then(function(allResultBuffers) {
        
        var     index;
        var     results;

        // extract the request results
        
        results = [];
        index = 0;
        allResultBuffers.forEach(function(resultBuffer) {
            resultBuffer.body.forEach(function(parseResult) {
                var     operation;
                var     result;

                result = {};
                operation = operations[index++];
                result.operation = operation;
                switch(operation.type) {
                    case "CREATE":
                        result.id = parseResult.success.objectId;
                        break;
                    case "DELETE":
                    case "UPDATE":
                        break;
                }
                results.push(result);
            });
        });

        return results;
    });
};