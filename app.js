'use strict';

var config = require('./config').config;
var raintankClient = require('./raintank-api-client');
var util = require('util');
var queue = require('raintank-queue');
var consumer = new queue.Consumer({
    mgmtUrl: config.queue.mgmtUrl,
    consumerSocketAddr: config.queue.consumerSocketAddr
});
var producer = queue.Publisher;
var hashCode = require('string-hash');
var async = require('async');
var numCPUs = config.numCPUs;
var http = require('http');
var cluster = require('cluster');
var url = require('url');
var zlib = require('zlib');

producer.init({
    publisherSocketAddr: config.queue.publisherSocketAddr,
    partitions: config.queue.partitions,
});

function handler (req, res) {
  res.writeHead(404);
  res.end();
}

function refresh(sockets) {
    if (! util.isArray(sockets)) {
        sockets = [sockets];
    }
    /*TODO:(awoods)
    We need to rewrite this to handle multiple sockets for each location.
    When > 1 socket for a location, each socket should get a portion of the
    monitors to run.  ie, if there are 3 sockets, each socket gets a third.
    */
    sockets.forEach(function(socket) {
        var filter = {
            location_id: socket.request.location.id,
        }
        socket.request.apiClient.get('monitors', filter, function(err, res) {
            if (err) {
                console.log("failed to get list of monitors for location %s", socket.request.location.slug);
                return console.log(err);
            }
            socket.emit('refresh', res.data);
        }); 
    });
}

if (cluster.isMaster) {
  // Fork workers.
  for (var i = 0; i < numCPUs; i++) {
    cluster.fork();
  }
  cluster.on('exit', function(worker, code, signal) {
    console.log('worker ' + worker.process.pid + ' died');
  });

} else {
    var app = http.createServer(handler)
    var io = require('socket.io')(app);
    app.listen(process.env.PORT || 8181);
    var RECV = 0;
    var BUFFER = {};

    setInterval(function() {
        var messages = BUFFER;
        BUFFER = {};
        var count = 0;
        var msgPayload = [];
        for (var id in messages) {
            msgPayload.push( {
                topic: "metrics",
                payload: messages[id],
                partition: id
            });
            count = count + messages[id].length;
        }
        if (msgPayload.length < 1) {
            return;
        }
        producer.batch(msgPayload);
    }, 100);
        

    setInterval(function() {
        var recv = RECV;
        RECV=0;
        console.log("RECV:%s metrics per sec", recv/10);
    }, 10000);


    io.use(function(socket, next) {
        var req  = url.parse(socket.request.url, true);
        var apiClient = new raintankClient({
            host: 'localhost',
            port: 3000,
            base: '/api/',
        });
        if (!('token' in req.query)) {
            console.log('connection attempt not authenticated.');
            next(new Error('Authentication error'));
        }
        apiClient.setToken(req.query.token);      
        apiClient.get('account', function(err, res) {
            if (err) {
                return next(err);
            }
            socket.request.account = res.data;
            socket.request.apiClient = apiClient;
            next();
        });
    });

    io.on('connection', function(socket) {
        console.log('new connection for user: %s@%s', socket.request.user, socket.request.locationCode);
        socket.on('serviceEvent', function(data) {
            zlib.inflate(data, function(err, buffer) {
                if (err) {
                    console.log("failed to decompress payload.");
                    console.log(err);
                    return;
                }
                var payload = buffer.toString();
                producer.send('serviceEvents', [payload]);
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
                    var partition = hashCode(metric.name) % config.queue.partitions;
                    if (!(partition in BUFFER)) {
                        BUFFER[partition] = [];
                    }
                    BUFFER[partition].push(metric);
                });
                RECV = RECV + count;
            });
        });
        socket.on('register', function(data) {
            console.log("register called.");
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
                        refresh(socket);
                    });
                } else if (res.data.length == 1) {
                    socket.request.location = res.data[0];
                    refresh(socket);
                }
            });
        });
    });

    setInterval(function() {
       refresh(io.sockets.sockets);
    }, 300000);

    var running = false;
    var client;
    var init = function() {
        consumer.on('connect', function() {
            consumer.join('serviceChange', 'locationManager:'+process.pid);
        });
        consumer.on('message', function (topic, partition, message) {
            message.forEach(function(msg) {
                var action = msg.action;
                console.log("got new message.");
                console.log(msg);
                msg.service.locations.forEach(function(loc) {
                    io.to(loc).emit(action, JSON.stringify(msg.service));
                });
            });
        });
    }

    init();
}
