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

function handler (req, res) {
  res.writeHead(404);
  res.end();
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
    var SENT = 0;
    var BUFFER = {};
    var publisher = new queue.Publisher({
        url: config.queue.url,
        exchangeName: "metricResults",
        exchangeType: "x-consistent-hash",
        retryCount: 5,
        retryDelay: 1000,
    });

    setInterval(function() {
        var messages = BUFFER;
        BUFFER = {};
        var msgPayload = [];
        for (var id in messages) {
            publisher.publish(JSON.stringify(messages[id]), id, function(err) {
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
        apiClient.get('users/accounts', function(err, res) {
            if (err) {
                return next(err);
            }
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

    var processMessage = function(message) {
        console.log(message);
        var routingKey = message.fields.routingKey;
        var action = routingKey.split('.')[1];
        var monitor = JSON.parse(message.content.toString());
        monitor.locations.forEach(function(loc) {
            //send event to the location responsible for this monitor.
            processAction(action, monitor, loc, io.sockets.sockets);
        });
    };

    var consumer = new queue.Consumer({
        url: config.queue.url,
        exchangeName: "monitors",  //this should match the name of the exchange the producer is using.
        exchangeType: "topic", // this should match the exchangeType the producer is using.
        queueName: '', //leave blank for an auto generated name. recommend when creating an exclusive queue.
        exclusive: true, //make the queue exclusive.
        durable: false,
        autoDelete: true,
        queuePattern: 'monitor.*', //match monitor.create, monitor.update, monitor.remove
        retryCount: -1, // keep trying to connect forever.
        handler: processMessage
    });
    
    
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
            var payload = {
                location: socket.request.location,
                services: res.data
            }
            socket.emit('refresh', payload);
        }); 
    });
}

function processAction(action, monitor, location, sockets) {
    /*TODO:(awoods)
    We need to rewrite this to handle multiple sockets for each location.
    When > 1 socket for a location, each socket should get a portion of the
    monitors to run.  ie, if there are 3 sockets, each socket gets a third.
    */
    sockets.forEach(function(socket) {
        if (socket.request.location.id == location ) {
            socket.emit(action, JSON.stringify(monitor));
        }         
    });
}