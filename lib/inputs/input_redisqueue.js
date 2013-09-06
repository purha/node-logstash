var base_input = require('../lib/base_input'),
    util = require('util'),
    redis_connection_manager = require('../lib/redis_connection_manager'),
    logger = require('log4node'),
    error_buffer = require('../lib/error_buffer');

function InputRedisqueue() {
  base_input.BaseInput.call(this);
  this.config = {
    name: 'Redis',
    host_field: 'host',
    port_field: 'port',
    required_params: ['key'],
    optional_params: ['error_buffer_delay', 'retry'],
    default_values: {
      'error_buffer_delay': 10000,
      'type': null,
    }
  }
  this.close_down = false;
}

util.inherits(InputRedisqueue, base_input.BaseInput);

InputRedisqueue.prototype.processRedisMessage = function(key, data) {
  try {
    var parsed = JSON.parse(data);
    if (this.type) {
      parsed['@type'] = this.type;
    }
    if (!parsed['@fields']) {
      parsed['@fields'] = {};
    }
    this.emit('data', parsed);
  }
  catch(e) {
    this.emit('error', 'Unable to parse data ' + data);
  }
}

InputRedisqueue.prototype.runblpop = function(client) {
    /*
     * Timeout after 2 secs, else redis quit will not work
     */
    client.blpop(this.key, 2, function(err, data) {
        if (data) {
            this.processRedisMessage(data[0], data[1]);
        }
        if (!this.close_down) {
            this.runblpop(client);
        }
    }.bind(this));
}

InputRedisqueue.prototype.afterLoadConfig = function(callback) {
  logger.info('Start listening Redis queue on', this.host + ':' + this.port, 'key', this.key);

  this.redis_connection_manager = redis_connection_manager.create(this.host, this.port);

  this.error_buffer = error_buffer.create('output Redis to ' + this.host + ':' + this.port, this.error_buffer_delay, this);

  this.redis_connection_manager.on('error', function(err) {
    this.error_buffer.emit('error', err);
  }.bind(this));

  this.redis_connection_manager.once('connect', function (client) {
      
      this.redis_connection_manager.on('before_quit', function(client) {
        this.close_down = true;
      }.bind(this));
      this.runblpop(client);
  }.bind(this));

  callback();
}

InputRedisqueue.prototype.close = function(callback) {
  this.redis_connection_manager.quit(callback);
}

exports.create = function() {
  return new InputRedisqueue();
}
