var azure = require('azure-storage');
var async = require('async');
var request = require('request');

var log = require('../lib/log');
var globalConfig = require('../config');
var config = globalConfig.svc;
var StatusCollector = require('../lib/status-collector');

var lastInactiveCheck = null;
var MAX_INACTIVE_TIME = 2; // Minutes
var initialized = false;

// Initialize environment, return config
function init(callback) {
  
  log.init({
    domain: process.env.COMPUTERNAME || '',
    instanceId: log.getInstanceId(),
    app: globalConfig.apps.orch.name,
    level: globalConfig.log.level,
    transporters: globalConfig.log.transporters
  },
    function(err) {
      if (err) {
        console.error(err);
        return callback(err);
      }
      return callback();
    });
}

function run(callback) {

  if (!config) {
    return context(new Error('Config is not set'));
  }

  // Making sure to update the *run every* x seconds
  RUN_EVERY = config.jobExecutionIntervalInSeconds;

  // 1. Check statuses
  console.info('Initializing statuses');
  var statusCollector = new StatusCollector(config);
  var hdinsightManager = statusCollector.hdinsightManager;

  return statusCollector.collect(function (err, status) {

    if (err) { return sendAlert({ error: error }); }
    if (status.queueError) { return sendAlert({ error: status.queueError }); }
    if (status.funcError) { return sendAlert({ error: status.funcError }); }
    if (status.hdinsightError) { return sendAlert({ error: status.hdinsightError }); }
    if (status.livyError) { return sendAlert({ error: status.livyError }); }

    var appServiceClient = statusCollector.appServiceClient;

    // Queue not empty
    // ================
    // 2. If queue is not empty && HDInsight is ResourceNotFound ==> create HDInsight
    console.info('If queue is not empty && HDInsight is ResourceNotFound ==> create HDInsight');
    if (status.queueLength > 0 && status.hdinsightStatus == 'ResourceNotFound') {
      console.log('Creating hdinsight');
      return hdinsightManager.createHDInsight(function (err) {
        if (err) { sendAlert({ error: err }); }
        console.log('Operation completed successfully');
        return context();
      })
    }

    // 3. If queue is not empty && HDInsight is operational && Livy is alive && function is down ==> wake up function
    console.info('If queue is not empty && HDInsight is Running && Livy is alive && function is down ==> wake up function');
    if (status.queueLength > 0 && status.hdinsightOperational && !status.funcActive) {
      console.log('Starting proxy app');
      return appServiceClient.start(function (err) {
        if (err) { sendAlert({ error: err }); }
        console.log('Operation completed successfully');
        return context();
      });
    }

    // Queue is empty
    // ================
    // 4. If queue is empty && hdinsight = ResourceNotFound && function is up
    // This state is illigal and might happen after first deployment ==> shut down functions
    console.info('If queue is empty && Livy jobs == 0 && hdinsight = ResourceNotFound && function is up');
    if (status.queueLength === 0 && status.hdinsightStatus == 'ResourceNotFound' && status.funcActive) {
        console.log('Stopping proxy app');
        return appServiceClient.stop(function (err) {
          if (err) { sendAlert({ error: err }); }
          console.log('Operation completed successfully');
          return context();
        })
    }

    // 5. If queue is empty && Livy jobs == 0 && function is up | more than 15 minutes ==> shut down functions
    console.info('If queue is empty && Livy jobs == 0 && function is up | more than 15 minutes ==> shut down functions');
    if (status.queueLength === 0 && status.livyRunningJobs === 0 && status.hdinsightOperational && status.funcActive) {
      var now = new Date();
      if (!lastInactiveCheck) {
        lastInactiveCheck = now;
        console.log('Operation completed successfully - initialized check time');
        return context();
      }

      var minutesPassed = getMinutes(now - lastInactiveCheck);
      console.log('Minutes passed since inactivity of function app: ' + minutesPassed);
      if (minutesPassed >= MAX_INACTIVE_TIME) {
        console.log('Stopping proxy app');
        lastInactiveCheck = null;
        return appServiceClient.stop(function (err) {
          if (err) { sendAlert({ error: err }); }
          console.log('Operation completed successfully');
          return context();
        });
      } else {
        return context();        
      }
    }
    
    // 6. If queue is empty && Livy jobs == 0 && function is down | more than 15 minutes ==> shut down HDInsight
    console.info('If queue is empty && Livy jobs == 0 && function is down | more than 15 minutes ==> shut down HDInsight');
    if (status.queueLength === 0 && status.livyRunningJobs === 0 && status.hdinsightOperational && !status.funcActive) {
      var now = new Date();
      if (!lastInactiveCheck) {
        lastInactiveCheck = now;
        console.log('Operation completed successfully - initialized check time');
        return context();
      }

      var minutesPassed = getMinutes(now - lastInactiveCheck);
      console.log('Minutes passed since inactivity of hdinsight: ' + minutesPassed);
      if (minutesPassed >= MAX_INACTIVE_TIME) {
        console.log('Deleting HDInsight cluster');
        return hdinsightManager.deleteHDInsight(function (err) {
          if (err) { 
            sendAlert({ error: err }); 
          }
          else {
            lastInactiveCheck = null; // If after 15 minutes hdinsight not down, try to delete again
          }
          console.log('Operation completed successfully');
          return context();
        })
      } else {
        return context();        
      }
    }

    return context();    
  });

  function sendAlert(alert) {

    console.error('ALERT: ' + alert);

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

module.exports = function (context, myTimer) {

  context.log('running orchestration...');
  console.log('running orchestration...');

  if (!initialized) {
    return init(function (err) {
      if (err) {
        return context.done(err);
      }

      initialized = true;
      return execute();
    });
  }

  return execute();

  function execute() {
    run(function (err) {

      if (err) {
        context.error('Error during execution: ' + err);
        return context.done(err);
      }

      context.log('Execution completed');
      return context.done();

    });
  }
};
