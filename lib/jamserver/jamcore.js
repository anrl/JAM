'use strict';

//===================================================================
// This is the core component for J node processing.
// All processing is linked to this component..
//===================================================================
const jsys = require('./jamsys');
const globals = require('./constants').globals;
const mqtt = require('mqtt');
const cbor = require('cbor');
const ebus = require('./ebus');
const deviceParams = require('./deviceparams');
const mqttconsts = require('./constants').mqtt;
const cmdopts = require('./cmdparser');
const JAMP = require('./jamprotocol');


const JCoreAdmin = require('./jcoreadmin');
const JCoreDaemon = require('./jcoredaemon');
const JCoreClient = require('./jcoreclient');
const RunTable = require('./runtable');
const NodeCache = require('./nodecache');

class JAMCore {

    constructor(reg, mtype) {

        // Maintain only one instance of the JAMCore.. for the system
        if (JAMCore.this)
            return JAMCore.this;
        JAMCore.this = this;

        this.reggie = reg;
        this.machtype = mtype;
        this.runLevel = [];
        this.bcasters = new Map();
        this.loggers = new Map();
        this.flows = new Map();

        this.resMap = new Map();
        this.results = [];
        this.cNodeCount = 0;

        this.jobQueue = new Array();
        this.workerBusy = false;

        this.ncache = this.getNcache(jsys.link);

        // setup some important data structures
        this.funcRegistry = new Map();
        this.copts = {
                    will: {topic: "/" + cmdopts.app + "/admin/announce/all",
                        payload: cbor.encode({"cmd": "KILL", "opt": "ALL", "cond": "-", "condvec": 0, "actname": "-", "actid": "-", "actarg": "-", "args": []}),
                        qos: 1,
                        retain: 0},
                    clientId: deviceParams.getItem('deviceId'),
                    keepalive: mqttconsts.keepAlive,
                    clean: false,
                    connectTimeout: mqttconsts.connectionTimeout,
                };

        this.fserv = null;
        this.cserv = null;

        // Start the core extensions
        this.jclient = new JCoreClient(this);
        this.jadmin = new JCoreAdmin(this);
        this.jdaemon = new JCoreDaemon(this);
    }

    run() {

        if (this.machtype === globals.NodeType.CLOUD) {
            this.reggie.registerAndDiscover();
            this.startService();
            this.startRunner();

        } else {
            this.jadmin.doRegister();
            this.startService();
            this.startRunner();
        }


    }



    // TODO: Include a scheduler that would pick the next job
    setWorker(worker) {

        this.worker = worker;
        var that = this;

        // Jobs already queued.. send one of them.
        if (this.jobQueue.length > 0) {
            this.worker.postMessage(this.jobQueue.shift());
            this.workerBusy = true;
        }

        // this.worker.onmessage = function(val) {
        //     that.processWorkerMsg(val.data);
        //     if (that.jobQueue.length > 0) {
        //         that.worker.postMessage(that.jobQueue.shift());
        //         that.workerBusy = true;
        //     } else
        //         that.workerBusy = false;
        // }

        // the main thread recieve msg from worker thread(jworklib.js)
      // worker.on/ worker.postMessgae is to receive/send msg from/to worker thread
      this.worker.on('message', (val)=> {
        // console.log("message from wroker(jworklib) to main thread (jamcore)"+val.cmd);
         that.processWorkerMsg(val);
         if (that.jobQueue.length > 0) {
             that.worker.postMessage(that.jobQueue.shift());
             that.workerBusy = true;
         } else
             that.workerBusy = false;
     });
     
        
        // relay data store up/down messages to the worker side 
        this.ncache.onFogDataUp(function(info) {
            that.worker.postMessage({cmd: 'NCACHE-MOD', opt: 'FOG-DATA-UP', data: info});
        });

        this.ncache.onFogDataDown(function() {
            that.worker.postMessage({cmd: 'NCACHE-MOD', opt: 'FOG-DATA-DOWN'});
        });

        this.ncache.onCloudDataUp(function(info) {
        //    console.log("---------- CLOUD DATA UP-----------");
            that.worker.postMessage({cmd: 'NCACHE-MOD', opt: 'CLOUD-DATA-UP', data: info});
        });

        this.ncache.onCloudDataDown(function() {
         //   console.log("---------- CLOUD DATA UP-----------");
            that.worker.postMessage({cmd: 'NCACHE-MOD', opt: 'CLOUD-DATA-DOWN'});
        });

    }

    processWorkerMsg(r) {

        switch (r.cmd) {
            case 'REXEC-ASY':
                this.jclient.remoteAsyncExec(r.name, r.params, r.expr, r.vec, r.bclock, r.count, r.mlevel);
                break;
            case 'REXEC-SYN':
                this.jclient.remoteSyncExec(r.name, r.params, r.expr, r.vec, r.bclock, r.count, r.mlevel, r.cbid);
                break;
            case 'MEXEC-ASY':
                this.jclient.machAsyncExec(r.name, r.params, r.expr, r.vec, r.bclock, r.count, r.mlevel);
                break;
            case 'MEXEC-SYN':
                this.jclient.machSyncExec(r.name, r.params, r.expr, r.vec, r.bclock, r.count, r.level, r.cbid);
                break;
            case 'REXEC-RES':
                this.jdaemon.sendReply(r.actid, r.data);
                break;
            case 'MEXEC-RES':
                var nmsg = {cmd: "MEXEC-RES", opt: this.machtype, 
                    cond: "-", condvec: 0, actname: r.name,
                    actid: r.actid, actarg: "RESULTS", args: [r.data]};
                JAMP.sendMachReply(this.mserv, this.fserv, this.cserv, this.machtype,  cmdopts.app, nmsg);
            break;
            case 'REXEC-ERR':
                console.log("ERROR posted.. for ", r.actid);
            break;
            case 'DATA-UP':
                jsys.setRedis(r.host, r.port);
                ebus.dataUp(r.host, r.port);
                jsys.adUp('datadepot', {host: r.host, port: r.port});
            break;
            case 'SET-CONF':
                if (r.opt === 'SET-LAT')
                    jsys.setLat(r.data);
                else if (r.opt === 'SET-LONG')
                    jsys.setLong(r.data);
            break;
        }
    }

    enqueueJob(job) {

        if (this.workerBusy)
            this.jobQueue.push(job);
        else {
            if (this.worker !== undefined) {
                this.worker.postMessage(job);
                this.workerBusy = true;
            } else
                this.jobQueue.push(job);
        }
    }

    pushErrorToWorker(opt, cbid, msg) {
        this.enqueueJob({cmd: 'REXEC-ERR', opt: opt, cbid: cbid, data: msg});
    }


    pushResultsToWorker(cbid, data) {
        this.enqueueJob({cmd: 'REXEC-RES', cbid: cbid, data: data});
    }

    registerCallback(name, fk, mask) {

        if (name === "" || name === undefined) {
            console.log("Anonymous functions cannot be callbacks... request ignored");
            return;
        }

        if (this.funcRegistry.has(name)) {
            console.log("Duplicate registration: " + name + ".. overwritten.");
        }

        this.funcRegistry.set(name, {func: fk, mask:mask});
    }

    startService() {

        // Changes
        var IDmap = new Map();
        this.mserv = mqtt.connect("tcp://localhost:" + cmdopts.port, this.copts);
        var that = this;
        // Setup the runTable
        this.runTable = new RunTable(this);

        this.mserv.on('connect', function() {
            that.mserv.subscribe('/' + cmdopts.app + '/admin/request/all');
            that.mserv.subscribe('/' + cmdopts.app + '/level/func/request');
            that.mserv.subscribe('/' + cmdopts.app + '/mach/func/reply');
            // Changes
            that.mserv.subscribe('/' + cmdopts.app + '/mach/func/syncrequest');

            // Send a ping every 500 milliseconds. This is needed so that the clients waiting at the
            // broker will know about a newly joining broker
            //
            doPing(that.mserv);
            setInterval(function() {
                doPing(that.mserv);
            }, 10000);

        });
        this.mserv.on('reconnect', function() {
            that.mserv.subscribe('/' + cmdopts.app + '/admin/request/all');
            that.mserv.subscribe('/' + cmdopts.app + '/level/func/request');
            that.mserv.subscribe('/' + cmdopts.app + '/mach/func/reply');
            // Changes
            that.mserv.subscribe('/' + cmdopts.app +'/mach/func/syncrequest');

            // Send a ping every 500 milliseconds. This is needed so that the clients waiting at the
            // broker will know about a newly joining broker
            //
            setInterval(function() {
                var tmsg = {"cmd": "PING", "opt": "BROKER", "cond": "-", "condvec": 0, "actname": "-", "actid": "-", "actarg": "-", "args": []};
                var encode = cbor.encode(tmsg);
                process.stdout.write(".");
                that.mserv.publish('/' + cmdopts.app + '/admin/announce/all', encode);
            }, 50000);
        });

        this.mserv.on('message', function(topic, buf) {
            cbor.decodeFirst(buf, function(error, msg) {

                switch (topic) {
                    case '/' + cmdopts.app + '/admin/request/all':
                    // Requests are published here by nodes under this broker
                        try {
                            that.jadmin.adminService(msg, function(rmsg) {
                                var encode = cbor.encode(rmsg);
                                that.mserv.publish('/' + cmdopts.app +'/admin/announce/all', encode);
                            });
                        } catch (e) {
                            console.log("ERROR!: ", e);
                        }
                    break;

                    case '/' + cmdopts.app + '/level/func/request':
                    // These are requests by the C nodes under this broker
                    // The requests are published from device and fog levels
                        try {
                            that.jdaemon.levelService(msg, function(rmsg) {
                                var encode = cbor.encode(rmsg);
                                that.mserv.publish('/' + cmdopts.app +'/level/func/reply/' + rmsg["actarg"], encode);
                            });
                        } catch (e) {
                            console.log("ERROR!: ", e);
                        }
                    break;

                    case '/' + cmdopts.app + '/mach/func/reply':
                    // Replies are bubbling upwards. So these are publications
                    // coming from submachines.
                        try {
                            that.jdaemon.machService(msg);
                        } catch (e) {
                            console.log("ERROR!: ", e);
                        }
                    break;

                    // Changes
                    case '/' + cmdopts.app + '/mach/func/syncrequest' :
                        try {
                           // adminService(msg, function(rmsg) {
                            //console.log(msg);
                            // Use the map to record the different nodes that are ready
                            IDmap.set(msg.actid, 1);
                            // All of the nodes are ready
                            if (IDmap.size >= that.cNodeCount) {
                                var now = new Date().getTime()/1000.0;
                                // the exact time that all C nodes should start their jobs
                                var now1 = now + 0.2;
                                var strNow = ''+ now1;
                                that.mserv.publish('/' + cmdopts.app + '/mach/func/syncstart', strNow);
                                IDmap.clear();
                            }
                        } catch(e) {
                            console.log("ERROR!: ", e);
                        }

                    break;

                }
            });
        });
    }

    startRunner() {
        var sock;

        // Every node listens to the up messages.
        this.runnerSockConfig2(this.mserv);

        // Cloud does not need the runner.
        if (this.machtype === globals.NodeType.CLOUD)
            return;

        if (this.machtype === globals.NodeType.FOG) 
            sock = this.cserv;
        else
            sock = this.fserv;

        // No need to run startRunner.. if the device is not connected to the fog..
        // Or the fog is not connected to the cloud..
        if (sock === undefined)
            return;

        this.runnerSockConfig(sock);
    }

    runnerSockConfig2(lsock) {

        var that = this;

        lsock.on('connect', function() {
            lsock.subscribe('/' + cmdopts.app + '/mach/func/urequest');
        });
        lsock.on('reconnect', function() {
            lsock.subscribe('/' + cmdopts.app +'/mach/func/urequest');
        });

        lsock.on('message', function(topic, buf) {

            cbor.decodeFirst(buf, function(error, msg) {
                switch (topic) {
                    case '/' + cmdopts.app + '/mach/func/urequest':
                    // Request for this node from up..
                        try {
                            that.jdaemon.machRunner(lsock, msg);
                        } catch (e) {
                            console.log("ERROR!: ", e);
                        }
                    break;
                }
            });
        });
    }

    runnerSockConfig(sock) {

        var that = this;

        if (sock === null || sock === undefined)
            return;

        sock.on('connect', function() {
            sock.subscribe('/' + cmdopts.app + '/mach/func/request');
        });
        sock.on('reconnect', function() {
            sock.subscribe('/' + cmdopts.app +'/mach/func/request');
        });

        sock.on('message', function(topic, buf) {

            cbor.decodeFirst(buf, function(error, msg) {
                switch (topic) {
                    case '/' + cmdopts.app + '/mach/func/request':
                    // Requests flowing downwards.
                        try {
                            that.jdaemon.machRunner(sock, msg);
                        } catch (e) {
                            console.log("ERROR!: ", e);
                        }
                    break;
                }
            });
        });
    }


    getMaxBcastClock(bstr) {

        var barr = [];
        var bcasters = eval(bstr);
        if (bcasters === undefined)
            return "-";

        var bclock;
        bcasters.forEach(function(b) {
            var tclock = this.bcasters.get(b).getClock();
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

    // Returns true if I am Root
    iamRoot(n) {

        switch (n & 0x7) {
            // JCond did not specify a level
            case 0:
            // Return true if I am cloud
            if (this.machtype == globals.NodeType.CLOUD)
                return true;
            // Return true if I am fog and no cloud
            if (this.machtype == globals.NodeType.FOG && this.cserv === null)
                return true;
            // Return true if I am device and no fog and no cloud
            if (this.machtype == globals.NodeType.DEVICE && this.cserv === null && this.fserv === null)
                return true;
            break;

            // JCond wants device
            case 1:
            if (this.machtype == globals.NodeType.DEVICE)
                return true;
            break;

            // JCond wants Fog
            case 2:
            if (this.machtype == globals.NodeType.FOG)
                return true;
            break;

            // JCond wants Cloud
            case 4:
            if (this.machtype == globals.NodeType.CLOUD)
                return true;
            break;
        }
        // Otherwise return false
        return false;
    }

    popLevel() {
        this.runLevel.pop();
    }

    pushLevel(level) {
        this.runLevel.push(level);
    }

    curLevel() {
        var tl = this.runLevel[this.runLevel.length -1];
        return tl;
    }

    addBroadcaster(name, obj) {
        this.bcasters.set(name, obj);
    }

    addLogger(name, obj) {
        this.loggers.set(name, obj);
    }

    addFlow(name, obj) {
        this.flows.set(name, obj)
    }

    getNcache() {
        if (this.ncache === undefined) {
            this.ncache = new NodeCache(200, 2, 2000, jsys.link);
        }
        return this.ncache;
    }
}

function doPing(serv) {

    var tmsg = {"cmd": "PING", "opt": "BROKER", "cond": "-", "condvec": 0, "actname": "-", "actid": "-", "actarg": "-", "args": []};
    var encode = cbor.encode(tmsg);
    serv.publish('/' + cmdopts.app +'/admin/announce/all', encode);
}


module.exports = JAMCore;
