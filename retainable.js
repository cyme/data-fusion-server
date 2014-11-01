module.exports = Retainable;

var ServerError = require('./server_error');


// Retainable
// {
//      refcount:       #refcount
//      onUnregister:   $function
// }

function Retainable()
{
    this.refcount = 0;
    this.onUnregister = null;
}

Retainable.prototype.retain = function()
{
    if (this.refcount === 0)
        this.register();
    this.refcount++;
    return this;
};

Retainable.prototype.release = function()
{
    this.refcount--;
    if (this.refcount === 0) {
        if (this.onUnregister !== null)
            this.onUnregister();
        this.unregister();
    }
    if (this.refcount < 0) {
        console.log('retain error');
        throw new ServerError("Retainable.release: unbalanced call to release");
    }
};
