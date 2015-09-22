/**

 Copyright 2015 Covistra Technologies Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
"use strict";

var P = require('bluebird'),
    Scheduler = require('redis-scheduler'),
    later = require('later'),
    _ = require('lodash');

exports.deps = ['covistra-messaging'];

exports.register = function (server, options, next) {

    var workers = {};

    server.log(['plugin', 'info'], "Registering the scheduler plugin");

    var Router = server.plugins['covistra-system'].Router;

    // Retrieve a reference to the current system configuration
    var config = server.plugins['hapi-config'].CurrentConfiguration;
    var log = server.plugins['covistra-system'].systemLog.child({plugin: 'scheduler'});

    var scheduler = new Scheduler({host: config.get('REDIS_URL'), port: config.get("REDIS_PORT"), auth: config.get("REDIS_PASSWORD") });

    // Ensure that we're receiving Keyevent from Redis
    scheduler.clients.scheduler.config("SET", "notify-keyspace-events", "Ex");

    // Expose a few methods to manage jobs
    plugin.expose('schedule', scheduler.schedule.bind(scheduler));
    plugin.expose('scheduleWorker', function(workerKey, expiration, jobKey) {
        jobKey = jobKey || _.uniqueId(workerKey);
        log.debug("Scheduling worker %s at %s (%s)", workerKey, expiration, jobKey);
        var shed;

        function recurHandler(err, jobKey) {
            log.debug("Handling job", arguments);
            scheduler.schedule({key: _.uniqueId(workerKey), expire: 50, handler: workers[workerKey]})
        }

        if(_.isString(expiration)) {
            // Compute execution schedule using later.js
            shed = later.parse.text(expiration);
            return P.resolve(later.setInterval(recurHandler, shed));
        }
        else {
            return P.promisify(scheduler.schedule, scheduler)({key: jobKey, expire: expiration, handler: workers[workerKey]});
        }
    });
    plugin.expose('registerWorker', function(workerKey, handler) {
        log.debug("Register a new worker %s", workerKey);
        workers[workerKey] = handler;
    });
    plugin.expose('addHandler', scheduler.addHandler.bind(scheduler));
    plugin.expose('reschedule', scheduler.reschedule.bind(scheduler));
    plugin.expose('cancel', scheduler.cancel.bind(scheduler));

    // Register routes
    Router.routes(plugin, __dirname, "./routes");

    next();
};

exports.register.attributes = {
    pkg: require('./package.json')
};
