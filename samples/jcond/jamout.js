var jamlib = require('/usr/local/share/jam/lib/jserver/jamlib');
var jnode = require('/usr/local/share/jam/lib/jserver/jnode');
var async = require('asyncawait/async');
var await = require('asyncawait/await');
var http = require('http');
var cbor = require('cbor');
var qs = require('querystring');
var path = require('path');
var mime = require('mime');
var fs = require('fs');
var jcondition = new Map();
jcondition.set('fogonly', { source: 'jcondition_context['sys.sync'] >= 10', code: 8 });
function pong() {
console.log("pong..");
ping();

}
var mbox = {
"functions": {
"pong": pong,
},
"signatures": {
"pong": "",
}
}
jamlib.registerFuncs(mbox);
jamlib.run(function() { console.log("Running..."); } );