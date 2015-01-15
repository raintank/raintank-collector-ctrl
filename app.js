'use strict';

var config = require('./config').config;
var schema = require('raintank-core/schema');
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
        //TODO: handle real user authentication so clients can deploy their own
        // collector nodes.
        if ('token' in req.query && req.query.token == config.adminToken) {
            socket.request.user = '531307ac1133a83d285b05a2';
            socket.request.account = '531307ac1133a83d285b05a1';
            socket.request.locationCode = req.query.location;
            next();
        } else {
            console.log('connection attempt not authenticated.');
            next(new Error('Authentication error'));
        }
    });

    io.on('connection', function(socket) {
        console.log('new connection for user: %s@%s', socket.request.user, socket.request.locationCode);
        socket.join(socket.request.locationCode);

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
            schema.locations.model.findOne({_id: data.id}).exec(function(err, location) {
                if (err) {
                    console.log("error looking up location");
                    console.log(err);
                    return;
                }
                console.log(location);
                if (!location) {
                    console.log('location not in DB.');
                    data._id = data.id;
                    data.account = socket.request.account;
                    location = new schema.locations.model(data);
                    location.save(function(err) {
                        if (err) {
                            console.log("error saving location.");
                            console.log(err);
                        }
                        console.log("saved location to DB.");
                    });
                }
                var filter = {
                    deletedAt: {$exists: false},
                    locations: location._id,
                }
                schema.services.model.find(filter).lean().exec(function(err, services){
                    socket.emit('refresh', JSON.stringify(services));
                });
            });
        });
    });

    setInterval(function() {
        schema.locations.model.find().lean().select('_id').exec(function(err, locations) {
            if (err) {
                console.log("ERROR getting location list.");
                console.log(err);
                return;
            }
            locations.forEach(function(loc) {
                var filter = {
                    deletedAt: {$exists: false},
                    locations: loc._id,
                }
                schema.services.model.find(filter).lean().exec(function(err, services){
                    console.log("refreshing serviceList for %s", loc._id);
                    io.to(loc._id).emit('refresh', JSON.stringify(services));
                });
            });
        });
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
