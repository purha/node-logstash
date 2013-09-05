var base_output = require('../lib/base_output'),
    util = require('util'),
    redis_connection_manager = require('../lib/redis_connection_manager'),
    logger = require('log4node'),
    error_buffer = require('../lib/error_buffer');

function OutputRedisqueue() {
  base_output.BaseOutput.call(this);
  this.config = {
    name: 'Redis',
    host_field: 'host',
    port_field: 'port',
    required_params: ['key'],
    optional_params: ['error_buffer_delay'],
    default_values: {
      'error_buffer_delay': 10000,
    }
  }
}

util.inherits(OutputRedisqueue, base_output.BaseOutput);

OutputRedisqueue.prototype.afterLoadConfig = function(callback) {
  logger.info('Start Redis output to', this.host + ':' + this.port, 'using key', this.key);

  this.redis_connection_manager = redis_connection_manager.create(this.host, this.port);

  this.error_buffer = error_buffer.create('output Redis to ' + this.host + ':' + this.port, this.error_buffer_delay, this);

  this.redis_connection_manager.on('error', function(err) {
    this.error_buffer.emit('error', err);
  }.bind(this));

  this.redis_connection_manager.once('connect', function(client) {
    this.client = client;
  }.bind(this));

  callback();
}

OutputRedisqueue.prototype.process = function(data) {
  if (this.client) {
    var key = this.replaceByFields(data, this.key);
    this.client.rpush(key, JSON.stringify(data));
  }
}

OutputRedisqueue.prototype.close = function(callback) {
  this.redis_connection_manager.quit(callback);
}

exports.create = function() {
  return new OutputRedisqueue();
}
