module.exports = UserError;

var SyncError = require('./sync_error');


function UserError(reason)
{
    SyncError.call(this, reason, false);
}

UserError.prototype = Object.create(SyncError.prototype);
UserError.prototype.constructor = UserError;
