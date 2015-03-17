'use strict';

var config = require('./config').config;
var raintankClient = require('./raintank-api-client');
var util = require('util');
var queue = require('raintank-queue');
var hashCode = require('crc-32').str;
var async = require('async');
var numCPUs = config.numCPUs;
var http = require('http');
var cluster = require('cluster');
var url = require('url');
var zlib = require('zlib');
var redis = require("redis");
var uuid = require('node-uuid');

var redisClient = redis.createClient(config.redis.port, config.redis.host);
redisClient.on("error", function (err) {
    console.log("redisClient Error " + err);
});

var HOSTID = uuid.v4();
var refreshLock = {};

function handler (req, res) {
  res.writeHead(404);
  res.end();
}

var metricPublisher;
var eventPublisher;
var collectorCtrlPublisher;
var io;
var ready = false;

if (cluster.isMaster) {
  // Fork workers.
  for (var i = 0; i < numCPUs; i++) {
    cluster.fork();
  }
  cluster.on('exit', function(worker, code, signal) {
    console.log('worker ' + worker.process.pid + ' died');
  });
  getNodesAlive(function(err, nodes) {
        //get list of collectors from redis
        getActiveCollectors(function(err, activeCollectors) {
            if (err) {
                console.log("failed to get list of activeCollectors.", err);
                return;
            }
            activeCollectors.forEach(function(collectorId) {
                getNodeSockets(collectorId, function(err, nodeSockets) {
                    if (err) {
                        console.log("failed to get list of nodes at collector: ", collectorId, err);
                        return;
                    }
                    var socketsToRemove = [];
                    nodeSockets.forEach(function(nodeSocket) {
                        var hostId = nodeSocket.split(".")[0];
                        if (!(hostId in nodes)) {
                            socketsToRemove.push(nodeSocket);
                        }
                    });
                    if (socketsToRemove.length > 0) {
                        delNodeSocket(collectorId, socketsToRemove, function(err) {
                            if (err) {
                                console.log("failed to remove stale nodeSockets.", err);
                                return;
                            }
                            refreshCollector(collectorId);
                        });
                    }
                });
            });
        });
    });

} else {
    var app = http.createServer(handler)
    io = require('socket.io')(app);
    setTimeout(function() {
        ready = true;
        console.log("starting server");
        app.listen(process.env.PORT || 8181);
    }, 3000);

    var RECV = 0;
    var SENT = 0;
    var BUFFER = {};
    metricPublisher = new queue.Publisher({
        url: config.queue.url,
        exchangeName: "metricResults",
        exchangeType: "x-consistent-hash",
        retryCount: 5,
        retryDelay: 1000,
    });
    eventPublisher = new queue.Publisher({
        url: config.queue.url,
        exchangeName: "grafana_events",
        exchangeType: "topic",
        retryCount: 5,
        retryDelay: 1000,
    });

    collectorCtrlPublisher = new queue.Publisher({
        url: config.queue.url,
        exchangeName: "collectorCtrlEvents",
        exchangeType: "fanout",
        retryCount: 5,
        retryDelay: 1000,
    });

    //send heartbeat to the queue.
    setInterval(function() {
        var payload = {
            type: "heartbeat",
            timestamp: new Date().getTime(),
            hostId: HOSTID,
        }
        collectorCtrlPublisher.publish(JSON.stringify(payload), '', function(err) {
            if (err) {
                console.log("failed to send heartbeat.", err);
                return;
            }
        });
    }, 500)

    setInterval(function() {
        var messages = BUFFER;
        BUFFER = {};
        var msgPayload = [];
        for (var id in messages) {
            metricPublisher.publish(JSON.stringify(messages[id]), id, function(err) {
                if (err) {
                    return console.log("failed to publish metrics.", err)
                }
                SENT = SENT + messages[id].length;
            });
        }
    }, 100);
        
    setInterval(function() {
        var recv = RECV;
        var sent = SENT;
        RECV=0;
        SENT=0;
        console.log("RECV:%s - SENT:%s metrics per sec", recv/10, sent/10);
    }, 10000);


    io.use(function(socket, next) {
        var req  = url.parse(socket.request.url, true);
        var apiClient = new raintankClient({
            host: config.api.host,
            port: config.api.port,
            base: config.api.path,
        });
        if (!('token' in req.query)) {
            console.log('connection attempt not authenticated.');
            next(new Error('Authentication error'));
        }
        apiClient.setToken(req.query.token);      
        apiClient.get('org', function(err, res) {
            if (err) {
                console.log("Authentication failed.", err)
                return next(err);
            }
            socket.request.apiClient = apiClient;
            socket.request.org = res.data;
            next();
        });
    });

    io.on('connection', function(socket) {
        console.log('new connection for org: %s', socket.request.org.name);
        socket.on('event', function(rawData) {
            function processEvent(data) {
                zlib.inflate(data, function(err, buffer) {
                    if (err) {
                        console.log("failed to decompress payload.");
                        console.log(err);
                        return;
                    }
                    var payload = buffer.toString();
                    var e = JSON.parse(payload);
                    if (!(socket.request.collector.public)) {      
                        e.org_id = socket.request.org.id;
                        payload = JSON.stringify(e);
                    }
                    var routing_key = util.format("EVENT.%s.%s", e.severity, e.event_type);
                    eventPublisher.publish(JSON.stringify(e), routing_key, function(err) {
                        if (err) {
                            console.log("Failed to send event to queue.", err);
                        } else {
                            console.log("Event sent to queue.");
                        }
                    });
                });
            }

            if ('collector' in socket.request) {
                processEvent(rawData);
            } else {
                setTimeout(function() {
                    processEvent(rawData);
                }, 2000)
            }
        });

        socket.on('results', function(data) {
            var process = function(data) {
                //if we recieve a result before the collector
                // has registered, then wait a second before processing.
                if (!("collector" in socket.request)) {
                    setTimeout(function() {
                        process(data);
                    }, 1000);
                    return;
                }
                zlib.inflate(data, function(err, buffer) {
                    if (err) {
                        console.log("failed to decompress payload.");
                        console.log(err);
                        return;
                    }
                    var payload = JSON.parse(buffer.toString());
                    var count =0;
                    payload.forEach(function(metric) {
                        count++;
                        // dont allow non-public collectors to send
                        // metrics for any org.
                        if (!(socket.request.collector.public)) {
                            metric.org_id = socket.request.org.id;
                        }
                        var partition = hashCode(metric.name) % 1024;
                        if (!(partition in BUFFER)) {
                            BUFFER[partition] = [];
                        }
                        BUFFER[partition].push(metric);
                    });
                    RECV = RECV + count;
                });
            }
            process(data);
        });
        socket.on('register', function(data) {
            if (!(data && 'name' in data)) {
                return socket.disconnect();
            }
            console.log("org %s registering collector %s.", socket.request.org.name, data.name);
            socket.request.apiClient.get('collectors', data, function(err, res) {
                if (err) {
                    console.log("failed to get collectors list.");
                    return socket.disconnect();
                }
                if (res.data.length > 1) {
                    console.log("multiple collectors returned.")
                    return socket.disconnect();
                } else if (res.data.length == 0) {
                    console.log("collector does not yet exist.  Creating it.");
                    socket.request.apiClient.put('collectors', data, function(err, res) {
                        if (err) {
                            console.log("failed to add new collector");
                            console.log(err);
                            return socket.disconnect();
                        }
                        console.log(res.data);
                        socket.request.collector = res.data;
                        register(socket);
                    });
                } else if (res.data.length == 1) {
                    socket.request.collector = res.data[0];
                    register(socket);
                }
            });
        });
        socket.on("disconnect", function(reason) {
            if ('collector' in socket.request) {
                console.log("collector %s disconnected. %s", socket.request.collector.name, reason);
                unregister(socket);
            } else {
                console.log("collector disconnected before registering.", reason);
            }
        });
    });

    // refresh all collector nodes every 5minutes
    setInterval(function() {
       refresh();
    }, 300000);

    
    var consumer = new queue.Consumer({
        url: config.queue.url,
        exchangeName: "grafana_events",  //this should match the name of the exchange the producer is using.
        exchangeType: "topic", // this should match the exchangeType the producer is using.
        queueName: '', //leave blank for an auto generated name. recommend when creating an exclusive queue.
        exclusive: true, //make the queue exclusive.
        durable: false,
        autoDelete: true,
        queuePattern: 'INFO.monitor.#', //match monitor.create, monitor.update, monitor.remove
        retryCount: -1, // keep trying to connect forever.
        handler: processGrafanaEvent
    });

    var consumer = new queue.Consumer({
        url: config.queue.url,
        exchangeName: "collectorCtrlEvents",  //this should match the name of the exchange the producer is using.
        exchangeType: "fanout", // this should match the exchangeType the producer is using.
        queueName: '', //leave blank for an auto generated name. recommend when creating an exclusive queue.
        exclusive: true, //make the queue exclusive.
        durable: false,
        autoDelete: true,
        queuePattern: '', //match monitor.create, monitor.update, monitor.remove
        retryCount: -1, // keep trying to connect forever.
        handler: processCollectorCtrlEvents
    });
}

function getActiveCollectors(callback) {
    redisClient.smembers("activeCollectors", callback);
}

function setActiveCollector(collectorId, callback) {
    redisClient.sadd("activecCllectors", collectorId, callback);
}

function getNodeSockets(collectorId, callback) {
    var key = util.format("collectorCtrl.%s", collectorId);
    console.log("sending query for nodeSockets");
    redisClient.smembers(key, callback);
}

function addNodeSocket(collectorId, socketId, callback) {
    var key = util.format("collectorCtrl.%s", collectorId);
    var value = util.format("%s.%s", HOSTID, socketId);
    console.log("adding nodeSocket %s to %s", value, key);
    redisClient.sadd(key, value, callback);
}

function delNodeSocket(collectorId, socketIds, callback) {
    var key = util.format("collectorCtrl.%s", collectorId);
    redisClient.srem(key, socketIds, callback);
}

function setNodeAlive(hostId, timestamp, callback) {
    redisClient.hset("nodeAlive", hostId, timestamp, callback);
}
function getNodesAlive(callback) {
    redisClient.hgetall("nodeAlive", callback);
}

function unsetNodeAlive(hostId, callback) {
    redisClient.hdel("nodeAlive", hostId, callback);
}

function processCollectorCtrlEvents(message) {
    var payload = JSON.parse(message.content.toString());
    if (payload.type == "heartbeat") {
        processHeartbeat(payload);
    } else if (payload.type == "nodeGone") {
        processNodeGone(payload);
    } else if (payload.type == "refresh") {
        refreshCollector(payload.collectorId);
    }
}

function processNodeGone(message) {
    if (message.hostId == HOSTID) {
        //our heartbeats are not being recieved.
        console.log("Our heartbeats are not being recieved. killing ourselves.")
        process.exit(1);
    }
    console.log("%s no longer active. cleaning up.", message.hostId);
    unsetNodeAlive(message.hostId, function(err) {
        if (err) {
            console.log("failed to remove node from NodeAlive list.",err);
        }
    });
    //get list of collectors from redis
    getActiveCollectors(function(err, activeCollectors) {
        if (err) {
            console.log("failed to get list of activeCollectors.", err);
            return;
        }
        activeCollectors.forEach(function(collectorId) {
            getNodeSockets(collectorId, function(err, nodeSockets) {
                if (err) {
                    console.log("failed to get list of nodes at collector: ", collectorId, err);
                    return;
                }
                var socketsToRemove = [];
                nodeSockets.forEach(function(nodeSocket) {
                    var hostId = nodeSocket.split(".")[0];
                    if (hostId == message.hostId) {
                        socketsToRemove.push(nodeSocket);
                    }
                });
                if (socketsToRemove.length > 0) {
                    delNodeSocket(collectorId, socketsToRemove, function(err) {
                        if (err) {
                            console.log("failed to remove stale nodeSockets.", err);
                            return;
                        }
                        refreshCollector(collectorId);
                    });
                }
            });
        });
    });
}


function processHeartbeat(message) {
    var now = new Date().getTime();
    var steps = [];
    //ignore the heartbeat if the message is older then 1second.
    if (message.timestamp > (now - 2000)) {
        steps.push(function(next) {
            setNodeAlive(message.hostId, message.timestamp, next)
        });
    }
    steps.push(function(next) {
        getNodesAlive(function(err, nodes) {
            if (err) {
                return next(err);
            }
            for (var hostId in nodes) {

                if (nodes[hostId] < (now - 2000)) {
                    var payload = {
                        type: "nodeGone",
                        hostId: hostId,
                    }
                    console.log("sending nodeGone event.", hostId);
                    collectorCtrlPublisher.publish(JSON.stringify(payload), '', function(err) {
                        if (err) {
                            console.log("failed to send nodeGone event.", err);
                            return;
                        }
                        console.log("nodeGone event published.", hostId);
                    });
                }
            }
            next();
        });
    });
    async.series(steps, function(err, results) {
        if (err) {
            console.log("processHeartbeat", err);
            return;
        }
    });  
}

function processGrafanaEvent(message) {
    console.log(message);
    var routingKey = message.fields.routingKey;
    var action = routingKey.split('.')[2];
    var monitor = JSON.parse(message.content.toString()).payload;
    console.log(monitor);
    monitor.collectors.forEach(function(c) {
        //send event to the collector responsible for this monitor.
	console.log("sending event to collector %s", c);
        processAction(action, monitor, c);
    });
}

function register(socket) {
    var collectorId = socket.request.collector.id;
    addNodeSocket(collectorId, socket.id, function(err) {
        if (err) {
            console.log("failed to add %s to collector %s", socket.id, collectorId);
            console.log(err);
            return;
        }
        var payload = {
            type: "refresh",
            collectorId: collectorId,
        }
        collectorCtrlPublisher.publish(JSON.stringify(payload), '', function(err) {
            if (err) {
                console.log("failed to send refresh to nodes.", err)
            }
        });
    });
    setActiveCollector(collectorId, function(err) {
        if (err) {
            console.log("failed to add %s to activeCollectors. ", collectorId, err);
        }
    });
}

function unregister(socket) {
    var collectorId = socket.request.collector.id;
    delNodeSocket(collectorId, util.format("%s.%s", HOSTID,  socket.id), function(err) {
        if (err) {
            console.log("failed to remove %s from collector %s", socket.id, collectorId);
            console.log(err);
            return;
        }
        var payload = {
            type: "refresh",
            collectorId: collectorId,
        }
        collectorCtrlPublisher.publish(JSON.stringify(payload), '', function(err) {
            if (err) {
                console.log("failed to send refresh to nodes.", err)
            }
        });
    })
}

function refresh() {
    if (! ready) {
        return;
    }
    getActiveCollectors(function(err, activeCollectors) {
        if (err) {
            console.log("failed to get list of active collectors. ", err);
            return;
        }
        activeCollectors.forEach(function(collectorId) {
            refreshCollector(collectorId);
        });
    });
}

function refreshCollector(collectorId) {
    if (! ready) {
        return;
    }
    if (refreshLock[collectorId]) {
        console.log("refresh is locked.");
        return;
    }
    refreshLock[collectorId] = true;
    setTimeout(function() {
        refreshLock[collectorId] = false;
        _refreshCollector(collectorId, function(err) {
            if (err) {
                console.log(err);
            }
        });
    }, 1000);
}

function _refreshCollector(collectorId, callback) {
    if (! ready) {
        return;
    }
    console.log("refreshing collector ", collectorId);
    getNodeSockets(collectorId, function(err, nodeSockets) {
        if (err) {
            console.log("failed to get list of nodeSockets.", err);
            return;
        }
        console.log(nodeSockets);
        var numSockets = nodeSockets.length;

        var count = -1;
        async.each(nodeSockets, function(nodeSocket, next) {
            count++;
            var node = nodeSocket.split(".")[0];
            if (node != HOSTID) {
                return next();
            }
            var socketId = nodeSocket.split(".")[1];
            async.each(io.sockets.sockets, function(socket, cb) {
                if (socket.id != socketId) {
                    return cb();
                }
                var filter = {
                    collector_id: collectorId,
                    modulo: numSockets,
                    modulo_offset: count % numSockets,
                }
                console.log(filter);
                socket.request.apiClient.get('monitors', filter, function(err, res) {
                    if (err) {
                        console.log("failed to get list of monitors for collector %s", socket.request.collector.slug);
                        console.log(err);
                        return cb();
                    }
                    var payload = {
                        collector: socket.request.collector,
                        services: res.data
                    }
                    console.log("sending %s to socket %s", payload.services.length, socket.id);
                    socket.emit('refresh', payload);
                    return cb();
                });
            }, function(err) {
                next();
            });
        }, function(err) {
            callback(err);
        });
    });
}

function processAction(action, monitor, collectorId) {
     getNodeSockets(collectorId, function(err, nodeSockets) {
        if (err) {
            console.log("failed to get list of nodeSockets.", err);
            return;
        }
        console.log(nodeSockets);
	if (nodeSockets.length < 1) {
		return;
	}
        var numSockets = nodeSockets.length;
	var nodePos = monitor.id % numSockets
 	var nodeSocket = nodeSockets[nodePos];
	var node = nodeSocket.split(".")[0];
        if (node != HOSTID) {
            return;
        }
	var socketId = nodeSocket.split(".")[1];

    	io.sockets.sockets.forEach(function(socket) {
		if ( socket.id != socketId) {
            		return;
		}
 		console.log("sending %s to %s", action, socket.id);
		socket.emit(action,JSON.stringify(monitor));
        });
    });
}
