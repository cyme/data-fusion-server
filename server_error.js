module.exports = ServerError;

var SyncError = require('./sync_error');



function ServerError(reason)
{
    SyncError.call(this, reason, true);
}

ServerError.prototype = Object.create(SyncError.prototype);
ServerError.prototype.constructor = ServerError;
