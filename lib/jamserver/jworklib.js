// jworklib.js

const deasync = require('deasync');
const globals = require('jamserver/constants').globals;
const ebus = require('jamserver/ebus');

const { parentPort}= require('worker_threads');
var funcRegistry = new Map();
var runLevel = [];
var bc = new Map();
var lg = new Map();
var fl = new Map();
var jcond = new Map();
var acount = 1;
var scount = 1;
var targets = new Array();
var waitMatrix = new Map();
var respondMatrix = new Map();

var jsys;
var cmdopts;

module.exports = new function() {
	this.registerFuncs = registerFuncs;
    this.run = run;
    this.poplevel = popLevel;
    this.addLogger = addLogger;
    this.addBroadcaster = addBroadcaster;
    this.addFlow = addFlow;

    this.remoteSyncExec = deasync(remoteSyncExec);
    this.remoteAsyncExec = remoteAsyncExec;
    this.machAsyncExec = machAsyncExec;
    this.machSyncExec = deasync(machSyncExec);
    this.setjcond = setjcond;
    this.getjsys = getjsys;
    this.getcmdopts = getcmdopts;
}

function setupJWorklib() {

    // Setup the targets...
    switch (jsys.type) {
        case 'cloud':
            targets.push('fog');
        break;
        case 'fog':
            targets.push('cloud', 'device');
        break;
        case 'device':
            targets.push('fog');
        break;
    }

    // Setup the ebus message handler so that we can react to events.
    ebus.on('data-up', function(x) {
        var msg = {cmd: 'DATA-UP', host: x.host, port: x.port};
        parentPort.postMessage(msg);
        // postMessage(msg);
    });
}

function getjsys() {
    return jsys;
}

function getcmdopts() {
    return cmdopts;
}

function popLevel() {
    runLevel.pop();
}

function pushLevel(level) {
    runLevel.push(level);
}

function curLevel() {
    var tl = runLevel[runLevel.length -1];
    return tl;
}

// We need to keep track of the broadcasters to determine the 
// broadcast clock. We need this value for the interlock - that is 
// we get the clock and send it with the call and the receiver would 
// wait for that data
function addBroadcaster(name, obj) {
    bc.set(name, obj);
}

// Similar use as the broadcaster? Need to check how it is used.
function addLogger(name, obj) {
    lg.set(name, obj);
}

function addFlow(name, obj) {
    fl.set(name, obj)
}

function setjcond(name, value) {
    jcond.set(name, value);
}


// after deasync, remoteSyncExec is called without the callback.
//
function remoteSyncExec(name, params, expr, vec, bcasters, callback) {

    // Get the maximum of the broadcaster clocks
    var bclock = getMaxBcastClock(bcasters);
    if (bclock === null) {
        callback(null, "Broadcast clock error");
    }

    var mlevel = curLevel();
    scount++;

    var cbid = name + scount; 

    var wmap = {device:true};
    var res = {};
    
    waitMatrix.set(cbid, {wmap:wmap, result: res, callback:callback, count:0, timeout:null});

    var msg = {cmd: 'REXEC-SYN', cbid: cbid, level: mlevel, name: name, params: params, expr: expr, vec: vec, bclock: bclock, count: scount};
    //postMessage(msg);
    parentPort.postMessage(msg);
}

// after deasync, machSyncExec is called without the callback 
//
function machSyncExec(name, params, oexpr, vec, bcasters, callback) {

    // Check if the function is already registered or not.
    // We cannot execute an unknown function!
    var fentry = funcRegistry.get(name);
    if (fentry === undefined) {
        callback(0, "Function not found: " + name);
    }

    // Get the maximum of the broadcaster clocks
    var bclock = getMaxBcastClock(bcasters);
    if (bclock === null)
        callback(null, "Broadcast clock error");

    // Get the level of access..
    var mlevel = 0b111 & vec;
    pushLevel(mlevel);
    var eres = jcondEval(oexpr);

    scount++;
    var cbid = "mach" + name + ":" + scount; 
    var msg = {cmd: 'MEXEC-SYN', cbid: cbid, level: mlevel, name: name, params: params, expr: oexpr, vec: vec, bclock: bclock, count: scount};
   // postMessage(msg);
   parentPort.postMessage(msg);

    // compute the wait map. this includes only other nodes.
    var wmap = {};
    var res = {};
    targets.forEach(function(t) {
        if (checkInterest(oexpr, t)) // && willr[t] > Math.random())
            wmap[t] = true;
    });

    // Do the local run..
    if (eres) 
        res[jsys.type] = fentry.func.apply(this, params);
    
    if (Object.keys(wmap).length == 0) 
        callback(null, res);
    else {
        // setup the timeout procedure..
        var ti = setTimeout(processTimeout, globals.Timeouts.J2J_CLOUD, cbid);
        var irec = {wmap:wmap, result: res, callback:callback, count:0, timeout:ti};
        // insert job parameters... 
        waitMatrix.set(cbid, irec);
    }
}


function remoteAsyncExec(name, params, expr, vec, bcasters) {

    // Get the maximum of the broadcaster clocks
    var bclock = getMaxBcastClock(bcasters);
    if (bclock === null)
        return;

    var mlevel = curLevel();
    // Global counter to track the asynchronous remote invocations
    acount++;

    var msg = {cmd: 'REXEC-ASY', name: name, params: params, expr: expr, vec: vec, bclock: bclock, count: acount};
    //postMessage(msg);
    parentPort.postMessage(msg);
}

// Returns true if it is able to launch the specified function
// Returns false if it determines a failure..
//
function machAsyncExec(name, params, oexpr, vec, bcasters) {

    // Check if the function is already registered or not.
    // We cannot execute an unknown function!
    var fentry = funcRegistry.get(name);
    if (fentry === undefined)
        return false;

    // Get the maximum of the broadcaster clocks
    var bclock = getMaxBcastClock(bcasters);
    if (bclock === null)
        return;

    var eres = jcondEval(oexpr);

    var msg = {cmd: 'MEXEC-ASY', name: name, params: params, expr: oexpr, vec: vec, bclock: bclock};
    //postMessage(msg);
    parentPort.postMessage(msg);

    if (eres)
        fentry.func.apply(this, params);
}

function registerCallback(name, fk, mask) {

    if (name === "" || name === undefined) {
        console.log("Anonymous functions cannot be callbacks... request ignored");
        return;
    }

    if (funcRegistry.has(name)) {
        console.log("Duplicate registration: " + name + ".. overwritten.");
    }

    funcRegistry.set(name, {func: fk, mask:mask});
}

function registerFuncs(machbox) {

	// Register all callbacks in the machbox.
	// These are functions registered by the application
	fkeys = Object.keys(machbox.functions);
	for (i in fkeys) {
		tkey = fkeys[i];
		registerCallback(tkey, machbox.functions[tkey], machbox.signatures[tkey]);
	}
}

function run(callback) {

    //onmessage = function(ev) { 
        //change tiny_worker to worker_thread
    //parentPort.on : receive msg from main thread
    //parentPort.postMessage : send msg from worker thread to main thread
    parentPort.on ("message",(v) =>{ 
       // var v = ev.data;
        var msg;
        switch (v.cmd) {
            case 'REXEC-ASY':
                asyncExecute(v, 'DONE', 'REXEC-ERR');
            break;

            case 'MEXEC-ASY':
                asyncExecute(v, 'DONE', 'MEXEC-ERR');
            break;

            case 'REXEC-SYN':
                syncExecute(v, 'REXEC-RES', 'REXEC-ERR');
            break;

            case 'MEXEC-SYN':
                syncExecute(v, 'MEXEC-RES', 'MEXEC-ERR');
            break;

            case 'REXEC-ERR':
                processWait(v.cbid, v.data, true);
                //Message({cmd: 'DONE'});
                parentPort.postMessage({cmd: 'DONE'});
            break;
            case 'REXEC-RES':
            case 'MEXEC-RES':
                processWait(v.cbid, v.data);
                //postMessage({cmd: 'DONE'});
                parentPort.postMessage({cmd: 'DONE'});
            break;
            case 'NCACHE-MOD':
                switch (v.opt) {
                    case 'FOG-DATA-UP':
                        ebus.fogDataUp(v.data);
                    break;
                    case 'FOG-DATA-DOWN':
                        ebus.fogDataDown();
                    break;

                    case 'CLOUD-DATA-UP':
                        ebus.cloudDataUp(v.data);
                    break;

                    case 'CLOUD-DATA-DOWN':
                        ebus.cloudDataDown();
                    break;
                }
                postMessage({cmd: 'DONE'});
            break;
            case 'CONF-DATA':
                switch (v.opt) {
                    case 'CMDOPTS':
                        cmdopts = v.data;
                    break;
                    case 'JSYS':
                        jsys = v.data; 
                        // inject some functions into jsys 
                        jsys.setLong = setLong;
                        jsys.setLat = setLat;
                        setupJWorklib();
                        // This is running the user program..                        
                        if (callback !== undefined)
                            callback();
                    break;
                }
                //postMessage({cmd: 'DONE'});
                parentPort.postMessage({cmd: 'DONE'});
            default:
        }
    }  );  
}

function asyncExecute(v, doneMsg, errMsg) {

    if (checkCondition(v.cond) !== true)
       // postMessage({cmd: doneMsg, data: "", actid: v.actid});
        parentPort.postMessage({cmd: doneMsg, data: "", actid: v.actid});
    else if (v.name !== undefined) {
        var fentry = funcRegistry.get(v.name);
        if (fentry !== undefined) {
            fentry.func.apply(this, v.args);
            //postMessage({cmd: doneMsg});
            parentPort.postMessage({cmd: doneMsg});
        } else 
            // postMessage({cmd: errMsg, data: "", actid: v.actid});
            parentPort.postMessage({cmd: errMsg, data: "", actid: v.actid});
    }   
}

function syncExecute(v, doneMsg, errMsg) {

    if (checkCondition(v.cond) !== true) {
        msg = {cmd: errMsg, data: "", actid: v.actid};
        //postMessage(msg);
        parentPort.postMessage(msg);
    } else if (v.name !== undefined) {
        var fentry = funcRegistry.get(v.name);
        if (fentry !== undefined) {
            res = fentry.func.apply(this, v.args);
            msg = {cmd: doneMsg, data: res, actid: v.actid}; // different from the async
            //postMessage(msg);
            parentPort.postMessage(msg);
        } else 
           // postMessage({cmd: errMsg, data: "", actid: v.actid});
            parentPort.postMessage({cmd: errMsg, data: "", actid: v.actid});
    }  
}


function setLong(val) {
    //postMessage({cmd: 'SET-CONF', opt: 'SET-LONG', data: val});
    parentPort.postMessage({cmd: 'SET-CONF', opt: 'SET-LONG', data: val});
}

function setLat(val) {
  //  postMessage({cmd: 'SET-CONF', opt: 'SET-LAT', data: val});
    parentPort.postMessage({cmd: 'SET-CONF', opt: 'SET-LAT', data: val});
}

function getMaxBcastClock(bstr) {

    var barr = [];
    var bcasters = eval(bstr);
    if (bcasters === undefined)
    return "-";

    var bclock;
    bcasters.forEach(function(b) {
        var tclock = bc[b].getClock();
        barr.push(b);
        if (bclock === undefined || tclock > bclock)
            bclock = tclock;
    });

    if (bclock === undefined)
        return "-";
    else if (bclock !== null)
        return barr.join(',') + '|' + bclock;
    else
        return bclock;
}

function jcondEval(cstr) {
    var rval;

    if (cstr.length === 0)
        return false;

    if (cstr.includes("&&") && cstr.includes("||")) {
        console.log("WARNING! Unsupported jcond condition. Use either && or ||.");
        return false;
    }

    if (cstr.includes("&&")) {
        var comps = cstr.split("&&");
        rval = true;
        comps.forEach(function(cexpr) {
            try {
                if (eval(eval(cexpr)) === false)
                    rval = false;
            } catch(e) {
                return false;
            }
        })
        return rval;
    }
    else if (cstr.includes("||")) {
        var comps = cstr.split("||");
        rval = false;
        comps.forEach(function(cexpr) {
            try {
                if (eval(eval(cexpr)) === true)
                    rval = true;
            } catch(e) {
                rval = false;
            }
        })
        return rval;
    }
    else
    {
        try {
            return eval(eval(cstr));
        } catch(e) {
            return false;
        }
    }
}

function checkCondition(cond) {

    if (cond === "")
        return true;
    var tstr = eval(cond);
    if (tstr === "")
        return true;

    return eval(tstr);
}

function jcondContext(clvar) {

    if (clvar.includes('.')) {
        return eval(clvar);
    }

    return null;
}



function processWait(cbid, data, error) {

    var we = waitMatrix.get(cbid);
    if (we === undefined) 
        return;

    var wmap = we.wmap;
    var res = we.result;

    // merge results.
    if (data['cloud'] !== null) {
        res['cloud'] = data['cloud'];
        delete(wmap.cloud);
    }
    if (data['fog'] !== null) {
        res['fog'] = data['fog'];
        delete(wmap.fog);
    }
    if (data['device'] !== null) {
        res['device'] = data['device'];
        delete(wmap.device);
    }

    if (Object.keys(wmap).length === 0) {

        clearTimeout(we.timeout);
        waitMatrix.delete(cbid);
        we.callback(null, we.result);
    }
}


function checkInterest(oexpr, t) {

    var oldtype = jsys.type;
    jsys.type = t;
    var rval = eval(oexpr);
    jsys.type = oldtype;
    return rval;
}

function processTimeout(pbid) {

    var cbid = pbid;
    var we = waitMatrix.get(cbid);
    if (we === undefined) 
        return;

    we.count++;

    if (we.count > 2) {
        Object.keys(we.wmap).forEach(function(x) {
            we.result[x] = null;
        });

        we.callback(null, we.result);
        waitMatrix.delete(cbid);        
    } else {
        var ti = setTimeout(processTimeout, globals.Timeouts.J2J_CLOUD, cbid);
        we.timeout = ti;
    }
}