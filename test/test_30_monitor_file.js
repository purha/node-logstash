var vows = require('vows'),
    assert = require('assert'),
    os = require('os'),
    fs = require('fs');
    path = require('path'),
    log = require('log4node'),
    monitor_file = require('monitor_file');

// log.setLogLevel('debug');

function TestMonitor(pathname, options) {
  this.file = path.join(pathname || os.tmpDir(), '___node-logstash_test___' + Math.random());
  this.lines = [];
  this.errors = [];
  this.init_errors = [];
  this.changed_counter = 0;
  this.renamed_counter = 0;
  this.closed_counter = 0;
  this.monitor = monitor_file.monitor(this.file, options);
  this.monitor.on('data', function(data) {
    this.lines.push(data);
  }.bind(this));
  this.monitor.on('error', function(err) {
    this.errors.push(err);
  }.bind(this));
  this.monitor.on('init_error', function(err) {
    this.init_errors.push(err);
  }.bind(this));
  this.monitor.on('renamed', function(err) {
    this.renamed_counter ++;
  }.bind(this));
  this.monitor.on('changed', function(err) {
    this.changed_counter ++;
  }.bind(this));
  this.monitor.on('closed', function(err) {
    this.closed_counter ++;
  }.bind(this));
}

function create_test(start_callback, check_callback, path, options) {
  return {
    topic: function() {
      var m = new TestMonitor(path, options);
      var callback = this.callback;
      start_callback(m, function(err) {
        m.monitor.close(function() {
          setTimeout(function() {
            callback(err, m);
          }, 50);
        });
      });
    },

    check: function(err, m) {
      assert.ifError(err);
      check_callback(m);
    }
  }
}

function no_error(m) {
  assert.equal(m.errors.length, 0);
  assert.equal(m.init_errors.length, 0);
}

vows.describe('Monitor ').addBatch({
  'Not existent file': create_test(
    function(m, callback) {
      m.monitor.start();
      setTimeout(callback, 200);
    }, function(m) {
      no_error(m);
      assert.equal(m.lines.length, 0);
    }
  ),
}).addBatch({
  'Empty file': create_test(
    function(m, callback) {
      fs.writeFileSync(m.file, '');
      m.monitor.start();
      setTimeout(callback, 200);
    }, function(m) {
      fs.unlinkSync(m.file);
      no_error(m);
      assert.equal(m.lines.length, 0);
    }
  ),
}).addBatch({
  'Not empty file start index undefined': create_test(
    function(m, callback) {
      fs.writeFileSync(m.file, 'line1\nline2\n');
      m.monitor.start();
      setTimeout(callback, 200);
    }, function(m) {
      fs.unlinkSync(m.file);
      no_error(m);
      assert.equal(m.lines.length, 0);
    }
  ),
}).addBatch({
  'Not empty file start index 0': create_test(
    function(m, callback) {
      fs.writeFileSync(m.file, 'line1\nline2\n');
      m.monitor.start(0);
      setTimeout(callback, 200);
    }, function(m) {
      fs.unlinkSync(m.file);
      no_error(m);
      assert.deepEqual(m.lines, ['line1', 'line2']);
    }
  ),
}).addBatch({
  'Not empty file start index 0, big buffer, and empty line removal': create_test(
    function(m, callback) {
      fs.writeFileSync(m.file, fs.readFileSync(__filename).toString());
      m.monitor.start(0);
      setTimeout(callback, 200);
    }, function(m) {
      fs.unlinkSync(m.file);
      no_error(m);
      var test_file_lines = fs.readFileSync(__filename).toString().split('\n');
      var index = 0;
      test_file_lines.forEach(function(l) {
        if (l.length > 0) {
          assert.equal(l, m.lines[index]);
          index += 1;
        }
      });
      assert.equal(m.lines.length, index);
    }
  ),
}).addBatch({
  'File filled after start': create_test(
    function(m, callback) {
      fs.writeFileSync(m.file, '');
      m.monitor.start();
      setTimeout(function() {
        fs.appendFileSync(m.file, 'line1\nline2\n');
        setTimeout(callback, 200);
      }, 200);
    }, function(m) {
      fs.unlinkSync(m.file);
      no_error(m);
      assert.deepEqual(m.lines, ['line1', 'line2']);
    }
  ),
}).addBatch({
  'File created after start': create_test(function(m, callback) {
    m.monitor.start(0);
    setTimeout(function() {
      fs.writeFileSync(m.file, 'line1\nline2\n');
      setTimeout(callback, 200);
    }, 200);
    }, function check(m) {
      fs.unlinkSync(m.file);
      no_error(m);
      assert.deepEqual(m.lines, ['line1', 'line2']);
    }
  ),
}).addBatch({
  'File created after start, filled with append': create_test(
    function(m, callback) {
      m.monitor.start();
      setTimeout(function() {
        fs.appendFileSync(m.file, 'line1\n');
        setTimeout(function() {
          fs.appendFileSync(m.file, 'line2\n');
          setTimeout(callback, 200);
        }, 200);
      }, 200);
    }, function(m) {
      fs.unlinkSync(m.file);
      no_error(m);
      assert.deepEqual(m.lines, ['line1', 'line2']);
    }
  ),
}).addBatch({
  'File created after start, filled with append async': create_test(
    function(m, callback) {
      m.monitor.start();
      setTimeout(function() {
        fs.appendFile(m.file, 'line1\n', function(err) {
          assert.ifError(err);
          setTimeout(function() {
            fs.appendFile(m.file, 'line2\n', function(err) {
              assert.ifError(err);
              setTimeout(callback, 200);
            });
          }, 200);
        });
      }, 200);
    }, function(m) {
      fs.unlinkSync(m.file);
      no_error(m);
      assert.deepEqual(m.lines, ['line1', 'line2']);
    }
  ),
}).addBatch({
  'File removed': create_test(function(m, callback) {
    fs.writeFileSync(m.file, 'line1\nline2\n');
    m.monitor.start(0);
    setTimeout(function() {
      fs.unlinkSync(m.file);
      setTimeout(callback, 200);
    }, 200);
    }, function check(m) {
      assert.equal(m.monitor.fdTailer, undefined);
      no_error(m);
      assert.deepEqual(m.lines, ['line1', 'line2']);
    }
  ),
}).addBatch({
  'File removed and recreated': create_test(function(m, callback) {
    fs.writeFileSync(m.file, 'line1\nline2\n');
    m.monitor.start(0);
    setTimeout(function() {
      fs.unlinkSync(m.file);
      setTimeout(function() {
        assert.equal(m.monitor.fdTailer, undefined);
        fs.writeFileSync(m.file, 'line3\n');
        setTimeout(callback, 200);
      }, 200);
    }, 200);
    }, function check(m) {
      no_error(m);
      assert.deepEqual(m.lines, ['line1', 'line2', 'line3']);
    }
  ),
}).addBatch({
  'Incomplete line': create_test(function(m, callback) {
    fs.writeFileSync(m.file, 'line1\nline2\nline3');
    m.monitor.start(0);
    setTimeout(function() {
      assert.deepEqual(m.lines, ['line1', 'line2']);
      setTimeout(function() {
        fs.appendFileSync(m.file, 'line3\nline4\nline5');
        setTimeout(callback, 200);
      }, 200);
    }, 200);
    }, function check(m) {
      fs.unlinkSync(m.file);
      no_error(m);
      assert.deepEqual(m.lines, ['line1', 'line2', 'line3line3', 'line4']);
    }
  ),
}).addBatch({
  'File filled while monitoring': create_test(function(m, callback) {
    m.test_fd = fs.openSync(m.file, 'a');
    var buffer = new Buffer('line1\nline2\n');
    m.monitor.start(0);
    setTimeout(function() {
      fs.writeSync(m.test_fd, buffer, 0, 6, null);
      fs.fsyncSync(m.test_fd);
      setTimeout(function() {
        assert.deepEqual(m.lines, ['line1']);
        fs.writeSync(m.test_fd, buffer, 6, 6, null);
        fs.fsyncSync(m.test_fd);
        setTimeout(callback, 200);
      }, 200);
    }, 200);
    }, function check(m) {
      fs.closeSync(m.test_fd);
      fs.unlinkSync(m.file);
      no_error(m);
      assert.deepEqual(m.lines, ['line1', 'line2']);
    }
  ),
}).addBatch({
  'double monitoring same directory': {
    topic: function() {
      var callback = this.callback;
      var m1 = new TestMonitor();
      var m2 = new TestMonitor();
      m1.monitor.start();
      m2.monitor.start();
      fs.appendFileSync(m1.file, 'line1\n');
      setTimeout(function() {
        fs.appendFileSync(m2.file, 'line10\n');
        setTimeout(function() {
          fs.appendFileSync(m1.file, 'line2\n');
          setTimeout(function() {
            m1.monitor.close(function() {
              m2.monitor.close(function() {
                setTimeout(function() {
                  callback(undefined, m1, m2);
                }, 50);
              });
            });
          }, 200);
        }, 200);
      }, 200);
    },

    check: function(err, m1, m2) {
      assert.ifError(err);
      fs.unlinkSync(m1.file);
      fs.unlinkSync(m2.file);
      no_error(m1);
      no_error(m2);
      assert.deepEqual(m1.lines, ['line1', 'line2']);
      assert.equal(m1.changed_counter, 2);
      assert.deepEqual(m2.lines, ['line10']);
      assert.equal(m2.changed_counter, 1);
    }
  }
}).addBatch({
  'Wrong file path': create_test(function(m, callback) {
    m.monitor.start(0);
    setTimeout(callback, 200);
    }, function check(m) {
      assert.equal(m.errors.length, 0);
      assert.equal(m.init_errors.length, 1);
      assert.equal(m.lines.length, 0);
    },
  '/toto_does_not_exists/toto.log'),
}).addBatch({
  'Simple logrotate simulation': create_test(function(m, callback) {
    m.monitor.start(0);
    setTimeout(function() {
      fs.writeFileSync(m.file, 'line1\nline2\n');
      setTimeout(function() {
        assert.deepEqual(m.lines, ['line1', 'line2']);
        fs.renameSync(m.file, m.file + '.1');
        fs.writeFileSync(m.file, 'line3\nline4\n');
        setTimeout(callback, 200);
      }, 200);
    }, 200);
    }, function check(m) {
      fs.unlinkSync(m.file);
      fs.unlinkSync(m.file + '.1');
      no_error(m);
      assert.deepEqual(m.lines, ['line1', 'line2', 'line3', 'line4']);
      assert.equal(m.closed_counter, 2);
    },
  undefined, {wait_delay_after_renaming: 1}),
}).addBatch({
  'Complex logrotate simulation': create_test(function(m, callback) {
    m.monitor.start(0);
    setTimeout(function() {
      fs.writeFileSync(m.file, 'line1\nline2\n');
      setTimeout(function() {
        assert.deepEqual(m.lines, ['line1', 'line2']);
        fs.renameSync(m.file, m.file + '.1');
        setTimeout(function() {
          fs.appendFileSync(m.file + '.1', 'line3\nline4\n');
          setTimeout(function() {
            fs.writeFileSync(m.file, 'line5\nline6\n');
            setTimeout(callback, 500);
          }, 100);
        }, 100);
      }, 200);
    }, 200);
    }, function check(m) {
      fs.unlinkSync(m.file);
      fs.unlinkSync(m.file + '.1');
      no_error(m);
      assert.deepEqual(m.lines, ['line1', 'line2', 'line3', 'line4', 'line5', 'line6']);
      assert.equal(m.closed_counter, 2);
    },
  undefined, {wait_delay_after_renaming: 500}),
}).export(module);
// Do not remove empty line, this file is used during test