var redis = require('redis'),
    crypto = require('crypto')
//
// QueueConsumer
//
// Create a new queue consumer that reads data off of the queue
// with `queue_name`
//

module.exports.QueueConsumer = QueueConsumer = function(queue_name) {
  this._queue_name = queue_name
  this._rcli = redis.createClient()
}

QueueConsumer.prototype = {

  //
  // listen(callback)
  //
  // Start listening for new events to be pushed to the queue, calling
  // the callback function every time new data is added to the queue.
  //
  listen : function(callback) {

    var self = this

    this._rcli.brpop(this._queue_name, 0, function(err, res) {

      var queue = res[0]
      var message = res[1]

      if (err) {

        conosle.log("Redis encountered an error: "+err)

      } else {

        var data;
        try {

          data = JSON.parse(message)

        } catch (e) {

            console.log("Entry is not proper JSON: "+message+" / "+e)

        }

        callback(data)

      }

      self.listen(callback)

    })

  }

}

//
// QueuePublisher
//
// Publishes data to a queue, also has helper methods to create queued
// jobs with callbacks.
//
module.exports.QueuePublisher = QueuePublisher = function(queue_name) {
  this._queue_name = queue_name
  this._rcli = redis.createClient()
  this._sub_rcli = redis.createClient()
  this._timeout_tokens = {}
  this._callbacks = {}

  // Code that handles queued callbacks.
  var self = this
  this._sub_rcli.on("message", function(channel, message) {

    clearTimeout(self._timeout_tokens[channel])
    delete self._timeout_tokens[channel]
    self._sub_rcli.unsubscribe(channel)

    try {

      var res_data = JSON.parse(message)
      self._callbacks[channel](res_data)
      delete self._callbacks[channel]

    } catch (e) {

      console.log("Unable to parse JSON data: "+e)
      callback_once(false)

    }

  })

}

QueuePublisher.prototype = {

  // publish raw data to a queue
  publish : function(data) {

    try {

      var message = JSON.stringify(data)
      this._rcli.lpush(this._queue_name, message, function(err) {

        if (err) {

          console.log("Encountered an error trying to publish a message: "+err)

        }

      })

    } catch (e) {
      console.log("Unable to serialize data: "+e)
    }

  },

  // default queued job timeout
  _default_timeout : 60*1000,

  // publish a job to the queue and call some function when the job completes
  publishJobWithCallback : function(job_type, data, _callback, timeout) {

    var self = this
    crypto.randomBytes(9, function(err,buffer) {

      var called = false,
          callback = _callback,
          callback_once = function(res){
            if (called) return
            called = true
            callback(res)
          },

          job_id = job_type+"_"+buffer.toString('base64')+"_"+Date.now(),

          timeout = timeout || self._default_timeout
      
      self._timeout_tokens[job_id] = setTimeout(callback_once, timeout)
      self._callbacks[job_id] = callback_once

      var job = {
        id : job_id,
        type : job_type,
        data : data
      }

      self.publish(job)
      self._sub_rcli.subscribe(job_id)

    })

  }

}
