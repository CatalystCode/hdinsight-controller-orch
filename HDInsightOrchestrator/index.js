var azure = require('azure-storage');
var async = require('async');
var request = require('request');

var logModule = require('../lib/log');
var globalConfig = require('../config');
var config = globalConfig.svc;
var StatusCollector = require('../lib/status-collector');

var lastInactiveCheck = null;
var MAX_INACTIVE_TIME = 2; // Minutes
var initialized = false;

// Initialize environment, return config
module.exports = function (context, myTimer) {

  log('running orchestration...');

  if (!initialized) {
    log('Initializing logging...')
    return init(function (err) {
      if (err) {
        error('Error initializing logging:', err);
        return context.done(err);
      }

      log('Initializing logging successfully');
      initialized = true;
      return execute();
    });
  }

  return execute();

  function log() {
    context.log.bind(this).call(arguments);
    if (console) {
      console.log.bind(this).call(arguments);      
    }
  }

  function error() {
    context.error.bind(this).call(arguments);
    if (console) {
      console.error.bind(this).call(arguments);      
    }
  }

  function info() {
    context.log.bind(this).call(arguments);
    if (console && console.info) {
      console.info.bind(this).call(arguments);      
    } else if (console && console.log) {
      console.log.bind(this).call(arguments);      
    }
  }

  function execute() {
    run(context, function (err) {

      if (err) {
        error('Error during execution: ' + err);
        return done(err);
      }

      log('Execution completed');
      return context.done();

    });
  }

  function init(callback) {
    
    logModule.init({
      domain: process.env.COMPUTERNAME || '',
      instanceId: logModule.getInstanceId(),
      app: globalConfig.apps.orch.name,
      level: globalConfig.log.level,
      transporters: globalConfig.log.transporters
    },
      function(err) {
        if (err) {
          error(err);
          return callback(err);
        }
        return callback();
      });
  }

  function run(context, callback) {

    if (!config) {
      return callback(new Error('Config is not set'));
    }

    // Making sure to update the *run every* x seconds
    RUN_EVERY = config.jobExecutionIntervalInSeconds;

    // 1. Check statuses
    info('Initializing statuses');
    var statusCollector = new StatusCollector(config);
    var hdinsightManager = statusCollector.hdinsightManager;

    return statusCollector.collect(function (err, status) {

      log('checking resulting status');
      if (err) { return sendAlert({ error: error }); }
      if (status.queueError) { return sendAlert({ error: status.queueError }); }
      if (status.funcError) { return sendAlert({ error: status.funcError }); }
      if (status.hdinsightError) { return sendAlert({ error: status.hdinsightError }); }
      if (status.livyError) { return sendAlert({ error: status.livyError }); }

      var appServiceClient = statusCollector.appServiceClient;

      // Queue not empty
      // ================
      // 2. If queue is not empty && HDInsight is ResourceNotFound ==> create HDInsight
      info('If queue is not empty && HDInsight is ResourceNotFound ==> create HDInsight');
      if (status.queueLength > 0 && status.hdinsightStatus == 'ResourceNotFound') {
        log('Creating hdinsight');
        return hdinsightManager.createHDInsight(function (err) {
          if (err) { sendAlert({ error: err }); }
          log('Operation completed successfully');
          return callback();
        })
      }

      // 3. If queue is not empty && HDInsight is operational && Livy is alive && function is down ==> wake up function
      info('If queue is not empty && HDInsight is Running && Livy is alive && function is down ==> wake up function');
      if (status.queueLength > 0 && status.hdinsightOperational && !status.funcActive) {
        log('Starting proxy app');
        return appServiceClient.start(function (err) {
          if (err) { sendAlert({ error: err }); }
          log('Operation completed successfully');
          return callback();
        });
      }

      // Queue is empty
      // ================
      // 4. If queue is empty && hdinsight = ResourceNotFound && function is up
      // This state is illigal and might happen after first deployment ==> shut down functions
      info('If queue is empty && Livy jobs == 0 && hdinsight = ResourceNotFound && function is up');
      if (status.queueLength === 0 && status.hdinsightStatus == 'ResourceNotFound' && status.funcActive) {
          log('Stopping proxy app');
          return appServiceClient.stop(function (err) {
            if (err) { sendAlert({ error: err }); }
            log('Operation completed successfully');
            return callback();
          })
      }

      // 5. If queue is empty && Livy jobs == 0 && function is up | more than 15 minutes ==> shut down functions
      info('If queue is empty && Livy jobs == 0 && function is up | more than 15 minutes ==> shut down functions');
      if (status.queueLength === 0 && status.livyRunningJobs === 0 && status.hdinsightOperational && status.funcActive) {
        var now = new Date();
        if (!lastInactiveCheck) {
          lastInactiveCheck = now;
          log('Operation completed successfully - initialized check time');
          return callback();
        }

        var minutesPassed = getMinutes(now - lastInactiveCheck);
        log('Minutes passed since inactivity of function app: ' + minutesPassed);
        if (minutesPassed >= MAX_INACTIVE_TIME) {
          log('Stopping proxy app');
          lastInactiveCheck = null;
          return appServiceClient.stop(function (err) {
            if (err) { sendAlert({ error: err }); }
            log('Operation completed successfully');
            return callback();
          });
        } else {
          return callback();        
        }
      }
      
      // 6. If queue is empty && Livy jobs == 0 && function is down | more than 15 minutes ==> shut down HDInsight
      info('If queue is empty && Livy jobs == 0 && function is down | more than 15 minutes ==> shut down HDInsight');
      if (status.queueLength === 0 && status.livyRunningJobs === 0 && status.hdinsightOperational && !status.funcActive) {
        var now = new Date();
        if (!lastInactiveCheck) {
          lastInactiveCheck = now;
          log('Operation completed successfully - initialized check time');
          return callback();
        }

        var minutesPassed = getMinutes(now - lastInactiveCheck);
        log('Minutes passed since inactivity of hdinsight: ' + minutesPassed);
        if (minutesPassed >= MAX_INACTIVE_TIME) {
          log('Deleting HDInsight cluster');
          return hdinsightManager.deleteHDInsight(function (err) {
            if (err) { 
              sendAlert({ error: err }); 
            }
            else {
              lastInactiveCheck = null; // If after 15 minutes hdinsight not down, try to delete again
            }
            log('Operation completed successfully');
            return callback();
          })
        } else {
          return callback();        
        }
      }

      return callback();    
    });

    function sendAlert(alert) {

      error('ALERT: ' + alert);

      var options = {
        uri: config.sendAlertUrl,
        method: 'POST',
        json: { alert: alert }
      };

      // Currently, not handling problems with alerts
      request(options);
    }

    function getMinutes(diffMs) {
      return Math.round(((diffMs % 86400000) % 3600000) / 60000);
    }
  }
};
