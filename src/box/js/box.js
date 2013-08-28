/* Alter 'exports' object here */
(function() {
    var lua = require('lua')
    /* Fallback to Lua for these bindings */
    exports.cfg = lua.box.cfg
    exports.info = lua.box.info
    exports.slab = lua.box.slab
    exports.stat = lua.box.stat
})();
