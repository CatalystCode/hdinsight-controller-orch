var azure = require('azure-storage');
var async = require('async');
var request = require('request');

module.exports = function (context, myTimer) {
    var timeStamp = new Date().toISOString();
    
    if(myTimer.isPastDue)
    {
        context.log('Node.js is running late!');
    }
    context.log('Node.js timer trigger function ran!', timeStamp);

    var config = null;
    try {
      config = require('./config');
    } catch (e) {
      return context.done(e);
    }   
    
    context.log('config was read ok');

    context.done();
};
