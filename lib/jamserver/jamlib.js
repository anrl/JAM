//===================================================================
// This is the main JAMLib module. It implements a generic set of
// functionality that makes it suitable for implementing the J nodes
// at the different levels: cloud, fog, and device.
//===================================================================

// Load the global modules
const mqtt = require('mqtt'),
	  os = require('os'),
      globals = require('./constants').globals;
//const Worker = require('tiny-worker');
const {Worker, isMainThread}= require('worker_threads');

// Do command line processing...
var cmdopts = require('./cmdparser');
// Get or make device parameters like device_id
var deviceParams = require('./deviceparams');
deviceParams.init(cmdopts.port);
var logger = require('./jerrlog');
logger.init(cmdopts.app, false);
// create the Registrar

var machType = getMachineType(cmdopts);
deviceParams.setItem('machType', machType);
// initialize the error logger
var Registrar = require('jdiscovery');
var reggie = new Registrar(cmdopts.app, machType, deviceParams.getItem('deviceId'),
						cmdopts.port, {long: cmdopts.long, lat: cmdopts.lat}, 
						{ protocols: { mqtt: false, mdns: true, localStorage: false } });

var jamsys = require('./jamsys');
jamsys.init(reggie, machType, cmdopts.tags, cmdopts.iflow, 
	cmdopts.oflow, deviceParams.getItem('deviceId'),
								cmdopts.link, cmdopts.long, cmdopts.lat);
jamsys.setMQTT(getMachineAddr(), cmdopts.port);
var jnode = require('./jnode');

if(isMainThread){

	//this code is excueted in the main thread and not in the wroker.
   
	//creqate the wroker
	const worker= new Worker("./jamout.js");
   
	//send data to worker thread
	worker.postMessage({cmd: 'CONF-DATA', opt:'CMDOPTS', data: cmdopts});
	worker.postMessage({cmd: 'CONF-DATA', opt:'JSYS', data: JSON.parse(JSON.stringify(jamsys))});

	
	//send worker to jamcore.js
	jnode.init(reggie, machType);
	var jcore = jnode.getcore();
	jcore.setWorker(worker);

	// var worker = new Worker('./jamout.js');
// worker.postMessage({cmd: 'CONF-DATA', opt:'CMDOPTS', data: cmdopts});
// worker.postMessage({cmd: 'CONF-DATA', opt:'JSYS', data: jamsys});
jnode.init(reggie, machType);
var jcore = jnode.getcore();
jcore.setWorker(worker);
   }else{
	 console.log("you are not the main thread")
   }




module.exports = new function() {
	this.registerFuncs = registerFuncs;
	this.run = run;
}

function run(callback) {

	// Check the presence of the MQTT server.
	// If not, report an error and quit

	checkMQTTServer(function(present) {
		if (!present) {
			console.log("ERROR! Cannot connect to MQTT server. Exiting.");
			process.exit(1);
		}
		jcore.run();

		if (callback !== undefined)
			callback();
	});

}

function registerFuncs(machbox) {

	// Register all callbacks in the machbox.
	// These are functions registered by the application
	fkeys = Object.keys(machbox.functions);
	for (i in fkeys) {
		tkey = fkeys[i];
		jnode.registerCallback(tkey, machbox.functions[tkey], machbox.signatures[tkey]);
	}
}

// Check whether the MQTT server is up and running..
function checkMQTTServer(callback) {
	var tserv = mqtt.connect("mqtt://localhost:" + cmdopts.port );
	tserv.on('connect', function() {
		tserv.end();
		callback(true);
	});
	tserv.on('offline', function() {
        tserv.end();
		callback(false);
	});
}


function getMachineType(copts) {

    if (copts.device) {
        machType = globals.NodeType.DEVICE;
    } else if (copts.fog) {
        machType = globals.NodeType.FOG;
    } else if (copts.cloud) {
        machType = globals.NodeType.CLOUD;
    } else {
		throw new 
		Error('no machine type specified - must be one of \'device\', \'fog\', or \'cloud\'');
    }

    return machType;
}


function getMachineAddr() {
    var niaddrs = os.networkInterfaces();
    for (var ni in niaddrs) {
        nielm = niaddrs[ni];
        for (n in nielm) {
            if (nielm[n].family === 'IPv4' && nielm[n].internal === false)
                return nielm[n].address
        }
    }
    return globals.localhost;
}
