'use strict';

var r = require('rethinkdb');
var async = require('async');
var assert = require('assert');

var dbConfig = {
  host: 'localhost',
  port: 28015,
  authKey: '',
  db: dbConfig.db,
  tables: {
    'users': 'id',
    'users_ops': 'id'
  }
};

var metaOperators = {
  $comment: true,
  $explain: true,
  $hint: true,
  $maxScan: true,
  $max: true,
  $min: true,
  $orderby: true,
  $returnKey: true,
  $showDiskLoc: true,
  $snapshot: true,
  $count: true
};

var cursorOperators = {
  $limit: 'limit',
  $skip: 'skip'
};

// This is an RethinkDB livedb database.
//
// There are 3 different APIs a database can expose. A database adaptor does
// not need to implement all three APIs. You can pick and choose at will.
//
// The three database APIs are:
//
// - Snapshot API, which is used to store actual document datay
//
// - Operation log for storing all the operations people have submitted. This
//   is used if a user makes changes while offline and then reconnects. Its
//   also really useful for auditing user actions.
//
// - Query API, which livedb wraps for live query capabilities.
//
// All databases should implement the close() method regardless of which APIs
// they expose.

// the way it is now.


function LiveDbRethink() {
  if (!(this instanceof LiveDbRethink)) {
    return new LiveDbRethink(dbConfig);
  }

  //TODO: Check if RethinkDB instance is availible.

  this.closed = false;

  // map from collection name -> true for op collections we've ensureIndex'ed
  this.opIndexes = {};

}

module.exports = LiveDbRethink;

LiveDbRethink.prototype.close = function(callback) {
  // Not implemented yet

  if (this.closed) {
    return callback('db already closed');
  }
  r.close(callback);
  this.closed = true;
};


function isValidCName(cName) {
  return !(/ops$/.test(cName)) && cName !== 'system';
}

LiveDbRethink.prototype._check = function(cName) {
  if (this.closed) {
    return 'db already closed';
  }
  // Todo: check if cName is valid.
  // if (!isValidCName(cName)) return 'Invalid collection name';

};


/* Snapshot methods */


// Get the named document from the database. The callback is called with (err,
// data). data may be null if the docuemnt has never been created in the
// database.
LiveDbRethink.prototype.getSnapshot = function(cName, docName, callback) {
  //console.log('getSnapshot', cName, docName);

  // TODO: Validate cName
  // var err; if (err = this._check(cName)) return callback(err);

  r.connect({
    host: dbConfig.host,
    port: dbConfig.port
  }, function(err, connection) {
    if (err) {
      console.log('DB CONNECTION FAILED: ', err);
    } else {
      r.db(dbConfig.db).table('users').get(docName).run(connection, function(err, doc) {
        if (err) {
          console.log('error', err);
        } else {
          //console.log('getSnapshot doc', doc);
          callback(err, castToSnapshot(doc));
        }
        connection.close();
      });
    }
  });
};

// Variant on getSnapshot (above) which projects the returned document
LiveDbRethink.prototype.getSnapshotProjected = function(cName, docName, fields, callback) {
  //console.log('getSnapshotProjected', cName, docName, fields);
  // TODO: Validate cName
  //var err; if (err = this._check(cName)) return callback(err);

  // This code depends on the document being stored in the efficient way (which is to say, we've
  // promoted all fields in rethink). This will only work properly for json documents - which happen
  // to be the only types that we really want projections for.
  var projection = projectionFromFields(fields);

  r.connect({
    host: dbConfig.host,
    port: dbConfig.port
  }, function(err, connection) {
    if (err) {
      console.log('DB CONNECTION FAILED: ', err);
    } else {
      r.db(dbConfig.db).table('users').get(docName).run(connection, projection, function(err, doc) {
        if (err) {
          console.log('error', err);
        } else {
          console.log('getSnapshot doc', doc);
          callback(err, castToSnapshot(doc));
        }
        connection.close();
      });
    }
  });
};

LiveDbRethink.prototype.bulkGetSnapshot = function(requests, callback) {
  //console.log('bulkGetSnapshot', requests);
  if (this.closed) {
    return callback('db already closed');
  }
  var results = {};

  var getSnapshots = function(cName, callback) {
    // TODO Validate cName
    //if (!isValidCName(cName)) {return 'Invalid collection name';}

    var cResult = results[cName] = {};
    var docNames = requests[cName];

    r.connect({
      host: dbConfig.host,
      port: dbConfig.port
    }, function(err, connection) {
      if (err) {
        console.log('DB CONNECTION FAILED: ', err);
      } else {
        // remember kids: arrays of ids can't be passed in the getAll function.
        // It must be a static arguement list, unless you use .apply()
        // and you have to pass the table as an argument to apply too. :/ FML
        var table = r.table('users');
        r.db(dbConfig.db).table('users').getAll.apply(table, docNames).run(connection, function(err, data) {
          if (err) {
            console.log('error', err);
          } else {
            data = data && data.map(castToSnapshot);
            for (var i = 0; i < data.length; i++) {
              cResult[data[i].docName] = data[i];
            }
            callback();
          }
          connection.close();
        });
      }
    });
  };

  async.each(Object.keys(requests), getSnapshots, function(err) {
    //console.log('results', results);
    callback(err, err ? null : results);
  });
};

LiveDbRethink.prototype.writeSnapshot = function(cName, docName, data, callback) {
  //console.log('writeSnapshot', cName, docName, data);
  //var err; if (err = this._check(cName)) return callback(err);
  var doc = castToDoc(docName, data);
  var docCreate = {
    id: doc.id,
    startLength: doc.startLength,
    endLength: doc.endLength,
    ops: doc.ops,
    type: doc.type,
    v: doc.v,
    m: doc.m,
  };
  // I don't think this is going to work but whatever... nothing works right now.
  r.connect({
    host: dbConfig.host,
    port: dbConfig.port
  }, function(err, connection) {
    if (err) {
      console.log('DB CONNECTION FAILED: ', err);
    } else {
      r.db(dbConfig.db).table('users').get(docName).run(connection, function(err, results) {
        if (err) {
          console.log('error', err);
        } else {
          if (!results) {
            doc.id = docName;
            //console.log('attempt to create doc', docCreate);
            r.db(dbConfig.db).table('users').insert(docCreate).run(connection, function(err, results) {
              if (err) {
                console.log('error', err);
              } else {
                //console.log('created doc', results);
                callback();
              }
            });
            connection.close();
          } else {
            //console.log('update', docName);
            r.db(dbConfig.db).table('users').get(docName).update(docCreate).run(connection, function(err, results) {
              if (err) {
                console.log('error', err);
              } else {
                //console.log('created doc', results);
                callback();
              }
              connection.close();
            });
          }
        }
        connection.close();
      });
    }
  });
};


/* Oplog methods*/

LiveDbRethink.prototype.getOplogCollectionName = function(cName) {
  //console.log('getOplogCollectionName', cName);
  // Using an underscore to make it easier to see whats going in on the shell
  return cName + '_ops';
};

// Get and return the op collection from mongo, ensuring it has the op index.
LiveDbRethink.prototype._opCollection = function(cName) {
  //console.log('_opCollection', cName);

};

LiveDbRethink.prototype.writeOp = function(cName, docName, opData, callback) {
  //console.log('writeOp', cName, docName, opData);

  assert(opData.v != null);

  // TODO validate cName
  // var err; if (err = this._check(cName)) return callback(err);
  //var self = this;
  //console.log('shallowClone', shallowClone(opData));
  var data = shallowClone(opData);

 // console.log('preValidate', data.preValidate);

  // hack to get around undefined insert errror in rethink.
  if (data.preValidate === undefined) {
    data.preValidate = {};
  }
  if (data.validate === undefined) {
    data.validate = {};
  }
  if (data.op === undefined) {
    data.op = {};
  }

  //console.log('data post shallowClone', data);
  //data._id = docName + ' v' + opData.v,
  data.name = docName;

  // I don't think this is going to work but whatever... nothing works right now.
  r.connect({
    host: dbConfig.host,
    port: dbConfig.port
  }, function(err, connection) {
    if (err) {
      console.log('DB CONNECTION FAILED: ', err);
    } else {
      r.db(dbConfig.db).table('users_ops').insert(data).run(connection, function(err, results) {
        if (err) {
          console.log('error', err);
        } else {
          //console.log('writeOp results', results);
          callback();
        }
        connection.close();
      });
    }
  });


  // this._opCollection(cName).save(data, callback);

};

LiveDbRethink.prototype.getVersion = function(cName, docName, callback) {
  //console.log('getVersion', cName, docName);
  // TODO validate cName
  // var err; if (err = this._check(cName)) return callback(err);

  // I don't think this is going to work but whatever... nothing works right now.
  r.connect({
    host: dbConfig.host,
    port: dbConfig.port
  }, function(err, connection) {
    if (err) {
      console.log('DB CONNECTION FAILED: ', err);
    } else {
      // if the docName doesn't exist in db, there may be problems with this.

      r.db(dbConfig.db).table('users').get(docName).pluck('v').run(connection, function(err, result) {

        if (err) {
          console.log('error', err);
        } else {
          //expect a single result or a null
          if (result === []) {
            callback(null, 0);
          } else {
            callback(err, result.v + 1);
          }
        }
        connection.close();
      });
    }
  });
};

LiveDbRethink.prototype.getOps = function(cName, docName, start, end, callback) {
  //console.log('getOps', cName, docName, start, end);
  // TODO validate cName
  //var err; if (err = this._check(cName)) return callback(err);

  if (end === null) {
    end = 'null';
  } else {
    end.toString();
  }

  start = start.toString();



  r.connect({
    host: dbConfig.host,
    port: dbConfig.port
  }, function(err, connection) {
    if (err) {
      console.log('DB CONNECTION FAILED: ', err);
    } else {
      // Here's the skinny:
      // There's a compound index used in RethinkDb that takes two sets of values, they are [docName, start] and [docName, end]
      // This will return as an unordered array.  It's up to you to sort it by version.
      // The hack employed here deals the 'end' object specifiaclly when it is passed to getOps as null. We change end value into a string and pass it on.
      // It's not great, however it works, and that's what counts right now.
      // Be aware that when end = null the whole array is coming back.
      // I need a strong drink.

      r.db(dbConfig.db).table('users_ops').between([docName, start], [docName, end], {

        index: 'operations',
        left_bound: 'closed',
        right_bound: 'closed'
      }).run(connection, function(err, cursor) {
        if (err) {
          console.log('error', err);
        } else {
          cursor.toArray(function(err, results) {
            if (err) {
              throw err;
            }
            // Sort the results according to version.
            var sorted = results.sort(function(a, b) {
              return a.v - b.v;
            });
            //console.log('sorted', sorted);
            callback(null, sorted);
          });

        }
      });
    }
    connection.close();
  });
};



/*Query methods*/

// Internal method to actually run the query.
LiveDbRethink.prototype._query = function(mongo, cName, query, fields, callback) {};

LiveDbRethink.prototype.query = function(livedb, cName, inputQuery, opts, callback) {};

LiveDbRethink.prototype.queryProjected = function(livedb, cName, fields, inputQuery, opts, callback) {};

LiveDbRethink.prototype.queryDocProjected = function(livedb, index, cName, docName, fields, inputQuery, callback) {};

LiveDbRethink.prototype.queryDoc = function(livedb, index, cName, docName, inputQuery, callback) {};

// Test whether an operation will make the document its applied to match the
// specified query. This function doesn't really have enough information to know
// in all cases, but if we can determine whether a query matches based on just
// the operation, it saves doing extra DB calls.
//
// currentStatus is true or false depending on whether the query currently
// matches. return true or false if it knows, or null if the function doesn't
// have enough information to tell.

LiveDbRethink.prototype.willOpMakeDocMatchQuery = function(currentStatus, query, op) {};

// Does the query need to be rerun against the database with every edit?
LiveDbRethink.prototype.queryNeedsPollMode = function(index, query) {};


/*Utility methods*/

// Return error string on error. Query should already be normalized with
// normalizeQuery below.
LiveDbRethink.prototype.checkQuery = function(query) {
  //console.log('checkQuery', query);
  if (!this.allowWhereQuery && query.$query.$where != null)
    return "Illegal $where query";
};

function extractCursorMethods(query) {
  //console.log('extractCursorMethods', query);
  var out = [];
  for (var key in query) {
    if (cursorOperators[key]) {
      out.push([cursorOperators[key], query[key]]);
      delete query[key];
    }
  }
  return out;
}

function normalizeQuery(inputQuery) {
  //console.log('normalizeQuery', inputQuery);
  // Box queries inside of a $query and clone so that we know where to look
  // for selctors and can modify them without affecting the original object
  var query;
  if (inputQuery.$query) {
    query = shallowClone(inputQuery);
    query.$query = shallowClone(query.$query);
  } else {
    query = {
      $query: {}
    };
    for (var key in inputQuery) {
      if (metaOperators[key] || cursorOperators[key]) {
        query[key] = inputQuery[key];
      } else {
        query.$query[key] = inputQuery[key];
      }
    }
  }

  // Deleted documents are kept around so that we can start their version from
  // the last version if they get recreated. When they are deleted, their type
  // is set to null, so don't return any documents with a null type.
  if (!query.$query.type) query.$query.type = {
    $ne: null
  };

  return query;
}

function castToDoc(docName, data) {
  //console.log('castToDoc', docName, data);
  var doc = (
      typeof data.data === 'object' &&
      data.data !== null &&
      !Array.isArray(data.data)
    ) ?
    shallowClone(data.data) : {
      data: (data.data === void 0) ? null : data.data
    };
  doc.type = data.type || null;
  doc.v = data.v;
  doc.m = data.m;
  doc.id = docName;
  //console.log('castToDoc result', doc);
  return doc;
}

function castToSnapshot(doc) {
  //console.log('castToSnapshot', doc);
  if (!doc) return;
  var type = doc.type;
  var v = doc.v;
  var docName = doc.id;
  var data = doc.data;
  var meta = doc.m;
  if (data === void 0) {
    doc = shallowClone(doc);
    delete doc.type;
    delete doc.v;
    delete doc.id;
    delete doc.m;
    return {
      data: doc,
      type: type,
      v: v,
      docName: docName,
      m: meta
    };
  }
  return {
    data: data,
    type: type,
    v: v,
    docName: docName,
    m: meta
  };
}

function shallowClone(object) {
  //console.log('shallowClone', object);

  var out = {};
  for (var key in object) {
    //console.log('object[key]', object[key]);
    out[key] = object[key];
  }
  return out;
}

// The fields property is already pretty perfect for mongo. This will only work for JSON documents.
function projectionFromFields(fields) {
  //console.log('projectionFromFields', fields);
  var projection = {};
  for (var k in fields) {
    projection[k] = 1;
  }
  projection.v = 1;
  projection.type = 1;
  projection.m = 1;

  return projection;

}

