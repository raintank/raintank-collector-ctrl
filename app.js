'use strict';

var config = require('./config').config;
var raintankClient = require('./raintank-api-client');
var util = require('util');
var queue = require('raintank-queue');
var hashCode = require('string-hash');
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
        //get list of locations from redis
        getActiveLocations(function(err, activeLocations) {
            if (err) {
                console.log("failed to get list of activeLocations.", err);
                return;
            }
            activeLocations.forEach(function(locationId) {
                getNodeSockets(locationId, function(err, nodeSockets) {
                    if (err) {
                        console.log("failed to get list of nodes at location: ", locationId, err);
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
                        delNodeSocket(locationId, socketsToRemove, function(err) {
                            if (err) {
                                console.log("failed to remove stale nodeSockets.", err);
                                return;
                            }
                            refreshLocation(locationId);
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
        apiClient.get('account', function(err, res) {
            if (err) {
                console.log("Authentication failed.", err)
                return next(err);
            }
            socket.request.apiClient = apiClient;
            socket.request.account = res.data;
            next();
        });
    });

    io.on('connection', function(socket) {
        console.log('new connection for account: %s', socket.request.account.name);
        socket.on('serviceEvent', function(data) {
            zlib.inflate(data, function(err, buffer) {
                if (err) {
                    console.log("failed to decompress payload.");
                    console.log(err);
                    return;
                }
                var payload = buffer.toString();
                //producer.send('serviceEvents', [payload]);
            });
        });

        socket.on('results', function(data) {
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
                    var partition = hashCode(metric.name) % 1024;
                    if (!(partition in BUFFER)) {
                        BUFFER[partition] = [];
                    }
                    BUFFER[partition].push(metric);
                });
                RECV = RECV + count;
            });
        });
        socket.on('register', function(data) {
            console.log("account %s registering location %s.", socket.request.account.name, data.name);
            socket.request.apiClient.get('locations', data, function(err, res) {
                if (err) {
                    console.log("failed to get locations list.");
                    return socket.disconnect();
                }
                if (res.data.length > 1) {
                    console.log("multiple locations returned.")
                    return socket.disconnect();
                } else if (res.data.length == 0) {
                    console.log("Location does not yet exist.  Creating it.");
                    socket.request.apiClient.put('locations', data, function(err, res) {
                        if (err) {
                            console.log("failed to add new location");
                            console.log(err);
                            return socket.disconnect();
                        }
                        console.log(res.data);
                        socket.request.location = res.data;
                        register(socket);
                    });
                } else if (res.data.length == 1) {
                    socket.request.location = res.data[0];
                    register(socket);
                }
            });
        });
        socket.on("disconnect", function(reason) {
            if ('location' in socket.request) {
                console.log("collector at location %s disconnected. %s", socket.request.location.name, reason);
                unregister(socket);
            } else {
                console.log("collector disconnected before registering location.", reason);
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

function getActiveLocations(callback) {
    redisClient.smembers("activeLocations", callback);
}

function setActiveLocation(locationId, callback) {
    redisClient.sadd("activeLocations", locationId, callback);
}

function getNodeSockets(locationId, callback) {
    var key = util.format("collectorCtrl.%s", locationId);
    console.log("sending query for nodeSockets");
    redisClient.smembers(key, callback);
}

function addNodeSocket(locationId, socketId, callback) {
    var key = util.format("collectorCtrl.%s", locationId);
    var value = util.format("%s.%s", HOSTID, socketId);
    console.log("adding nodeSocket %s to %s", value, key);
    redisClient.sadd(key, value, callback);
}

function delNodeSocket(locationId, socketIds, callback) {
    var key = util.format("collectorCtrl.%s", locationId);
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
        refreshLocation(payload.locationId);
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
    //get list of locations from redis
    getActiveLocations(function(err, activeLocations) {
        if (err) {
            console.log("failed to get list of activeLocations.", err);
            return;
        }
        activeLocations.forEach(function(locationId) {
            getNodeSockets(locationId, function(err, nodeSockets) {
                if (err) {
                    console.log("failed to get list of nodes at location: ", locationId, err);
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
                    delNodeSocket(locationId, socketsToRemove, function(err) {
                        if (err) {
                            console.log("failed to remove stale nodeSockets.", err);
                            return;
                        }
                        refreshLocation(locationId);
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
    monitor.locations.forEach(function(loc) {
        //send event to the location responsible for this monitor.
	console.log("sending event to location %s", loc);
        processAction(action, monitor, loc);
    });
}

function register(socket) {
    var locationId = socket.request.location.id;
    addNodeSocket(locationId, socket.id, function(err) {
        if (err) {
            console.log("failed to add %s to location %s", socket.id, locationId);
            console.log(err);
            return;
        }
        var payload = {
            type: "refresh",
            locationId: locationId,
        }
        collectorCtrlPublisher.publish(JSON.stringify(payload), '', function(err) {
            if (err) {
                console.log("failed to send refresh to nodes.", err)
            }
        });
    });
    setActiveLocation(locationId, function(err) {
        if (err) {
            console.log("failed to add %s to activeLocations. ", locationId, err);
        }
    });
}

function unregister(socket) {
    var locationId = socket.request.location.id;
    delNodeSocket(locationId, util.format("%s.%s", HOSTID,  socket.id), function(err) {
        if (err) {
            console.log("failed to remove %s from location %s", socket.id, locationId);
            console.log(err);
            return;
        }
        var payload = {
            type: "refresh",
            locationId: locationId,
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
    getActiveLocations(function(err, activeLocations) {
        if (err) {
            console.log("failed to get list of active locations. ", err);
            return;
        }
        activeLocations.forEach(function(locationId) {
            refreshLocation(locationId);
        });
    });
}

function refreshLocation(locationId) {
    if (! ready) {
        return;
    }
    if (refreshLock[locationId]) {
        console.log("refresh is locked.");
        return;
    }
    refreshLock[locationId] = true;
    setTimeout(function() {
        _refreshLocation(locationId, function(err) {
            console.log("refresh complete.", err);
            refreshLock[locationId] = false;
        });
    }, 1000);
}

function _refreshLocation(locationId, callback) {
    if (! ready) {
        return;
    }
    console.log("refreshing location ", locationId);
    getNodeSockets(locationId, function(err, nodeSockets) {
        if (err) {
            console.log("failed to get list of nodeSockets.", err);
            return;
        }
        console.log(nodeSockets);
        var numSockets = nodeSockets.length;

        var count = 0;
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
                    location_id: locationId,
                    modulo: numSockets,
                    modulo_offset: count % numSockets,
                }
                console.log(filter);
                socket.request.apiClient.get('monitors', filter, function(err, res) {
                    if (err) {
                        console.log("failed to get list of monitors for location %s", socket.request.location.slug);
                        console.log(err);
                        return cb();
                    }
                    var payload = {
                        location: socket.request.location,
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

function processAction(action, monitor, locationId) {
     getNodeSockets(locationId, function(err, nodeSockets) {
        if (err) {
            console.log("failed to get list of nodeSockets.", err);
            return;
        }
        console.log(nodeSockets);
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
