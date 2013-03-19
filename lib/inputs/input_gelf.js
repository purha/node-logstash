var base_input = require('../lib/base_input'),
    dgram = require('dgram'),
    util = require('util'),
    logger = require('log4node'),
    zlib = require('zlib');

function InputGELF() {
  base_input.BaseInput.call(this);
  this.config = {
    name: 'Gelf',
    host_field: 'host',
    port_field: 'port',
    optional_params: ['type', 'source'],
  }
}

util.inherits(InputGELF, base_input.BaseInput);

InputGELF.prototype.parseGELF = function(error, result) {
    var parsed = JSON.parse(result);
    if (!parsed['timestamp']) parsed['timestamp'] = (new Date()).getTime() / 1000;
    this.emit('data', parsed);
}

InputGELF.prototype.afterLoadConfig = function(callback) {
  logger.info('Start listening on GELF udp', this.host + ':' + this.port);

  this.server = dgram.createSocket('udp4');

  this.server.on('message', function(data, remote) {
    try {
      if (data[0] == 120 && data[1] == 156) { // ZLIB
        zlib.inflate(new Buffer(data), this.parseGELF.bind(this));
      } else if (data[0] == 31 && data[1] == 139) { // GZIP
        zlib.gunzip(new Buffer(data), this.parseGELF.bind(this));
      } else if (data[0] == 30 && data[1] == 15) { // Chunked
          // TODO
          this.parseGELF.bind(this);
      } else {
        // Try to parse plain data
        this.parseGELF.bind(this);
      }
    }
    catch(e) {
      logger.error('Message not in JSON format');
    }
    
  }.bind(this));

  this.server.on('error', function(err) {
    this.emit('init_error', err);
  }.bind(this));

  this.server.once('listening', callback);

  this.server.bind(this.port, this.host);
}

InputGELF.prototype.close = function(callback) {
  logger.info('Closing listening GELF udp', this.host + ':' + this.port);
  this.server.close();
  callback();
}

exports.create = function() {
  return new InputGELF();
}