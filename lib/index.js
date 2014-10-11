'use strict';

var fs = require('fs');
var cluster = require('cluster');
var _ = require('underscore');
var winston = require('winston');

var createLogger = function (logLevel) {
    var logger = new (winston.Logger)({
        transports: [
            new (winston.transports.Console)({level: logLevel})
        ]
    });

    return logger;
};

var getNumWorkers = function () {
    return Object.keys(cluster.workers).length;
};

var Master = function (clusterName, configPath) {
    this.clusterName = clusterName;
    this.configPath = configPath;
    this.workersByAge = [];
};

Master.prototype.loadConfig = function () {
    this.config = JSON.parse(fs.readFileSync(this.configPath));

    if (!_(this.config.clusters).isObject() ||
        !_(this.config.clusters[this.clusterName]).isObject()) {
        throw new Error('No configuration for cluster ' + this.clusterName);
    }

    this.clusterConfig = this.config.clusters[this.clusterName];
};

Master.prototype.spawnWorker = function () {
    var worker = cluster.fork();

    this.workersByAge.push(worker.id);

    worker.send({type: 'config',
                 config: this.config,
                 clusterName: this.clusterName});

    return worker;
};

Master.prototype.onExit = function (deadWorker) {
    var newWorker;

    if (deadWorker.suicide) {
        this.logger.info("A worker died, but it was a suicide. Phew.", {
            id: deadWorker.id,
        });
        return;
    } else {
        this.logger.error('A worker died unexpectedly.', {
            id: deadWorker.id,
        });
    }

    if (this.shuttingDown) {
        this.logger.info(
            'Cluster is shutting down. Not spawning replacement worker.');
        return;
    } else if (this.disabled) {
        this.logger.info(
            'Cluster is disabled. Not spawning replacement worker.');
        return;
    }

    if (getNumWorkers() >= this.clusterConfig.numWorkers) {
        this.logger.warn('Enough workers running. Not spawning replacement.', {
            numWorkers: numWorkers
        });
        return;
    }

    newWorker = this.spawnWorker();

    this.logger.info('Started replacement worker.', {
        id: newWorker.id,
    });

    this.logger.info('Workers are running.', {
        num: getNumWorkers()
    });
};

Master.prototype.onTerminate = function () {
    var oldWorker, newWorker, id;

    if (this.shuttingDown) {
        this.logger.info(
            'Cluster is shutting down. Not turning over old worker.');
        return;
    } else if (this.disabled) {
        this.logger.info(
            'Cluster is disabled. Not turning over old worker.');
        return;
    }

    do {
        id = this.workersByAge.shift();
        if (cluster.workers[id]) {
            oldWorker = cluster.workers[id];
            break;
        } else {
            this.logger.warn('Defunct worker in workersByAge.');
        }
    } while (this.workersByAge.length > 0);

    if (!oldWorker) {
        return;
    }

    this.logger.info('Informing worker to voluntarily disable itself.', {
        id: oldWorker.id,
        pid: oldWorker.process.pid,
    });

    oldWorker.send({type: 'disable'});
    newWorker = this.spawnWorker();

    this.logger.info('Started replacement worker.', {
        id: newWorker.id,
    });

    setTimeout(function () {
        oldWorker.destroy();
    }, this.clusterConfig.terminationGrace);
};

Master.prototype.onSIGUSR1 = function () {
    var id;

    this.logger.info('Caught SIGUSR1. Reloading config.');
    this.loadConfig();

    for (id in cluster.workers) {
        cluster.workers[id].send({
            type: 'config',
            config: this.config, 
            clusterName: this.clusterName 
        });
    }
};

Master.prototype.onSIGUSR2 = function () {
    var id;

    this.logger.info('Caught SIGUSR2. Disabling cluster.');
    this.disabled = true;

    for (id in cluster.workers) {
        cluster.workers[id].send({type: 'disable'});
    }
};

Master.prototype.onSIGTERM = function () {
    var id;

    this.logger.info('Caught SIGTERM. Shutting cluster down.');
    this.shuttingDown = true;

    for (id in cluster.workers) {
        cluster.workers[id].destroy();
    }

    process.exit(0);
};

Master.prototype.run = function () {
    this.loadConfig();
    this.logger = createLogger(this.clusterConfig.logLevel);

    this.logger.info('Cluster is starting up.', {
        name: this.clusterName,
        pid: process.pid,
    });

    fs.writeFileSync(this.clusterConfig.pidPath, process.pid);

    this.logger.info('Master is spawning initial workers.', {
        numWorkers: this.clusterConfig.numWorkers,
        pid: process.pid,
    });

    _(this.clusterConfig.numWorkers).times(this.spawnWorker, this);

    process.on('SIGUSR1', this.onSIGUSR1.bind(this));
    process.on('SIGUSR2', this.onSIGUSR2.bind(this));
    process.on('SIGTERM', this.onSIGTERM.bind(this));
    cluster.on('exit', this.onExit.bind(this));

    // Kill workers periodically to prevent them from being too comfy.
    // Actually, the real reason is to prevent memory leaks :p
    setInterval(this.onTerminate.bind(this),
                this.clusterConfig.terminationInterval);

    this.main();
};

Master.prototype.main = function () {

};

var Worker = function () {

};

Worker.prototype.handleConfigMessage = function (message) {
    var clusterName = message.clusterName;
    var config = message.config;
    var clusterConfig = config.clusters[clusterName];

    this.logger = createLogger(clusterConfig.logLevel);

    this.logger.info('Worker received config message.', {
        pid: process.pid,
        clusterName: clusterName,
    });

    this.onConfig(config);
};

Worker.prototype.handleDisableMessage = function (message) {
    this.logger.info('Worker received disable message.', {
        pid: process.pid,
    });

    this.onDisable();
};

Worker.prototype.onMessage = function (message) {
    var type = message.type;

    var handlers = {config: this.handleConfigMessage,
                    disable: this.handleDisableMessage};

    if (_(handlers).has(type)) {
        handlers[type].call(this, message);
    } else if (this.logger) {
        this.logger.warn('Worker received unknown message.', {
            message: JSON.stringify(message),
            pid: process.pid,
        });
    }
};

Worker.prototype.onConfig = function () {

};

Worker.prototype.onDisable = function () {

};

Worker.prototype.onSignal = function (signal) {
    this.logger.info('Worker supressing ' + signal + '.');
};

Worker.prototype.run = function () {
    var signals = ['SIGUSR1', 'SIGUSR2', 'SIGHUP', 'SIGTERM'];

    _(signals).each(function (signal) {
        process.on(signal, this.onSignal.bind(this, signal));
    }, this);
    
    process.on('message', _(this.onMessage).bind(this));
};

exports.createLogger = createLogger;
exports.Master = Master;
exports.Worker = Worker;