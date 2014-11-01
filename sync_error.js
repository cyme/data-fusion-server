module.exports = SyncError;

function SyncError(reason, internal)
{
    this.reason = reason;
    this.internal = internal;
console.log('ERROR!!! '+reason)
}

SyncError.prototype.isInternal = function()
{
    return this.internal;
};

SyncError.prototype.getReason = function()
{
    return this.reason;
};

SyncError.isError = function(object)
{
    return (object instanceof SyncError);
};

SyncError.errorObjectNotFound = "Object Not Found";

