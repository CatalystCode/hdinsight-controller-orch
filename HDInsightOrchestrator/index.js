module.exports = function (context, myQueueItem) {
    context.log('Node.js queue trigger function processed work item', myQueueItem);
    
    context.bindings.outputQueueItem = { 
      some_text: 'hello world', 
      data: myQueueItem };
    
    return context.done();

    // function postToLivy(callback) {
    //   var authenticationHeader = 'Basic ' + new Buffer(config.clusterLoginUserName + ':' + config.clusterLoginPassword).toString('base64');
    //   var options = {
    //     uri: 'https://' + config.clusterName + '.azurehdinsight.net/livy/batches',
    //     method: 'GET',
    //     headers: { "Authorization": authenticationHeader },
    //     json: { }
    //   };

    //   context.log('Checking livy state');
    //   request(options, function (err, response, body) {

    //     if (err || !response || response.statusCode != 200) {
    //       status.livyError = err ? err : !response ? 
    //         new Error ('No response received') :
    //         new Error ('Status code is not 200');
    //       return callback();
    //     }

    //     // Need to check validity and probably filter only running jobs
    //     status.livyJobs = response.batches.length;
    //     context.log('livy jobs: ' + status.livyJobs);
    //     return callback();
    //   });
    // }
};