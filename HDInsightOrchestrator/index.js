// var azure = require('azure-storage');
// var async = require('async');
// var request = require('request');

//var FunctionsManager = require('../lib/manage-functions');
//var HDInsightManager = require('../lib/manage-hdinsight');

var lastInactiveCheck = null;
var MAX_INACTIVE_TIME = 15; // Minutes

module.exports = function (context, myTimer) {
    var timeStamp = new Date().toISOString();
    
    context.log('Node.js timer trigger function ran!', timeStamp);

    context.done();
};
