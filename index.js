(function() {
  'use strict';
  // This is an RethinkDB livedb database.
  //
  // Its main use is as an API example for people implementing database adaptors.
  // This database is fully functional, except it stores all documents &
  // operations forever in memory. As such, memory usage will grow without bound,
  // it doesn't scale across multiple node processes and you'll lose all your
  // data if the server restarts. Use with care.
  //
  // There are 3 different APIs a database can expose. A database adaptor does
  // not need to implement all three APIs. You can pick and choose at will.
  //
  // The three database APIs are:
  //
  // - Snapshot API, which is used to store actual document data
  // - Query API, which livedb wraps for live query capabilities.
  // - Operation log for storing all the operations people have submitted. This
  //   is used if a user makes changes while offline and then reconnects. Its
  //   also really useful for auditing user actions.
  //
  // All databases should implement the close() method regardless of which APIs
  // they expose.


  var r = require('rethinkdb');
  var async = require('async');
  var assert = require('assert');

  // holds the RethinkDB connection config
  var dbConfig = {};

  // #### Helper functions
  /**
   * A wrapper function for the RethinkDB API `r.connect`
   * to keep the configuration details in a single function
   * and fail fast in case of a connection error.
   */
  function onConnect(callback) {
    r.connect({
      host: dbConfig.host,
      port: dbConfig.port,
      db: dbConfig.db
    }, function(err, connection) {
      assert.ok(err === null, err);
      connection._id = Math.floor(Math.random() * 10001);
      callback(err, connection);
    });
  }


  function create(cName, doc, callback) {
    onConnect(function(err, connection) {
      r.table(cName)
        .insert(doc)
        .run(connection)
        .then(function(result) {
          callback(null, result);
        })
        .error(function(err) {
          callback(err, null);
        });
      connection.close();
    });
  }

  function update(cName, docName, doc, callback) {
    onConnect(function(err, connection) {
      r.table(cName)
        .get(docName)
        .update(doc)
        .run(connection)
        .then(function(result) {
          callback(null, result);
        })
        .error(function(err) {
          callback(err, null);
        });
      connection.close();
    });
  }


  /*
  FROM LIVEDB-MONGO
   */

  // Utility methods

  // Return error string on error. Query should already be normalized with
  // normalizeQuery below.
  LiveDbRethink.prototype.checkQuery = function(query) {
    console.log('LiveDbRethink checkQuery');
    var err = 'Not implemented';
    return err;
    // if (!this.allowJSQueries) {
    //   if (query.$query.$where != null)
    //     return "$where queries disabled";
    //   if (query.$mapReduce != null)
    //     return "$mapReduce queries disabled";
    // }

    // if (!this.allowAggregateQueries && query.$aggregate)
    //   return "$aggregate queries disabled";
  };

  function extractCursorMethods(query) {
    console.log('LiveDbRethink extractCursorMethods');
    var err = 'Not implemented';
    return err;
    // var out = [];
    // for (var key in query) {
    //   if (cursorOperators[key]) {
    //     out.push([cursorOperators[key], query[key]]);
    //     delete query[key];
    //   }
    // }
    // return out;
  }

  function normalizeQuery(inputQuery) {
    console.log('LiveDbRethink normalizeQuery');
    var err = 'Not implemented';
    return err;
    // // Box queries inside of a $query and clone so that we know where to look
    // // for selctors and can modify them without affecting the original object
    // var query;
    // if (inputQuery.$query) {
    //   query = shallowClone(inputQuery);
    //   query.$query = shallowClone(query.$query);
    // } else {
    //   query = {$query: {}};
    //   for (var key in inputQuery) {
    //     if (metaOperators[key] || cursorOperators[key]) {
    //       query[key] = inputQuery[key];
    //     } else {
    //       query.$query[key] = inputQuery[key];
    //     }
    //   }
    // }

    // // Deleted documents are kept around so that we can start their version from
    // // the last version if they get recreated. When they are deleted, their type
    // // is set to null, so don't return any documents with a null type.
    // if (!query.$query._type) query.$query._type = {$ne: null};

    // return query;
  }

  function castToDoc(docName, data) {
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
    return doc;
  }

  function castToSnapshot(doc) {
    if (!doc) {
      return;
    }
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
    var out = {};
    for (var key in object) {
      out[key] = object[key];
    }
    return out;
  }

  // The fields property is already pretty perfect for mongo. This will only work for JSON documents.
  function projectionFromFields(fields) {
    console.log('LiveDbRethink projectionFromFields');
    var projection = {};
    for (var k in fields) {
      projection[k] = 1;
    }
    projection.v = 1;
    projection.type = 1;
    projection.m = 1;

    return projection;
  }


  exports = module.exports = function(rethink, options) {
    dbConfig = rethink;
    return new LiveDbRethink(rethink, options);
  };

  // Deprecated. Don't use directly.
  exports.LiveDbRethink = LiveDbRethink;

  function LiveDbRethink(rethink, options) {
    this.rethink = rethink;
    this.closed = false;

    if (!options) options = {};

    this.rethinkPoll = options.rethinkPoll || null;

    // The getVersion() and getOps() methods depend on a collectionname_ops
    // collection, and that collection should have an index on the operations
    // stored there. I could ask people to make these indexes themselves, but
    // even I forgot on some of my collections, so the rethink driver will just do
    // it manually. This approach will leak memory relative to the number of
    // collections you have, but if you've got thousands of rethink collections
    // you're probably doing something wrong.

    // map from collection name -> true for op collections we've ensureIndex'ed
    this.opIndexes = {};

    // Allow $while and $mapReduce queries. These queries let you run arbitrary
    // JS on the server. If users make these queries from the browser, there's
    // security issues.
    this.allowJSQueries = options.allowAllQueries || options.allowJSQueries || options.allowJavaScriptQuery || false;

    // Aggregate queries are less dangerous, but you can use them to access any
    // data in the rethink database.
    this.allowAggregateQueries = options.allowAllQueries || options.allowAggregateQueries;
  }

  LiveDbRethink.prototype.close = function(callback) {
    if (this.closed) return callback('db already closed');
    this.rethink.close(callback);
    this.closed = true;
  };

  // Snapshot database API

  function isValidCName(cName, callback) {
    onConnect(function(err, connection) {
      r.table(cName)
        .status()
        .run(connection)
        .then(function(result) {
          if (result.status.all_replicas_ready === 'true' && result.status.ready_for_outdated_reads === 'true' && result.status.ready_for_reads === 'true' && result.status.ready_for_writes === 'true') {
            callback(null, cName);
          } else {
            callback(false, null);
          }
        })
        .error(function(err) {
          callback(err, null);
        });
      connection.close();
    });
  }

  LiveDbRethink.prototype._check = function(cName) {
    if (this.closed) {
      return 'db already closed';
    }
    isValidCName(cName, function(err, result) {
      if (err) {
        return 'Invalid collection name', err;
      }
      return;
    });
  };


  // Get the named document from the database. The callback is called with (err,
  // snapshot). snapshot may be null if the docuemnt has never been created in the
  // database.
  LiveDbRethink.prototype.getSnapshot = function(cName, docName, callback) {
    var err;
    if (err = this._check(cName)) {
      return callback(err);
    }
    onConnect(function(err, connection) {
      r.table(cName)
        .get(docName)
        .run(connection)
        .then(function(result) {
          callback(null, castToSnapshot(result));
        })
        .error(function(err) {
          callback(err, null);
        });
      connection.close();
    });
  };

  LiveDbRethink.prototype.writeSnapshot = function(cName, docName, data, callback) {
    var err;
    if (err = this._check(cName)) return callback(err);
    var doc = castToDoc(docName, data);

    onConnect(function(err, connection) {
      r.table(cName)
        .get(docName)
        .run(connection)
        .then(function(result) {
          if (!result) {
            create(cName, doc, function(err, result) {
              if (err) {
                callback(err);
              }
              if (!result) {
                callback(err);
              }
              callback();
            });
          }
          if (result) {
            update(cName, docName, doc, function(err, result) {
              if (err) {
                callback(err);
              }
              if (!result) {
                callback(err);
              }
              callback();
            });
          }
        })
        .error(function(err) {
          callback(err, null);
        });
      connection.close();
    });
  };


  // This function is optional, but should be implemented for reducing the
  // number of database queries.
  //
  // Its included here for demonstration purposes and so we can test our tests.
  //
  // requests is an object mapping collection name -> list of doc names for
  // documents that need to be fetched.
  //
  // The callback is called with (err, results) where results is a map from
  // collection name -> {docName:data for data that exists in the collection}
  //
  // Documents that have never been touched can be ommitted from the results.
  // Documents that have been created then later deleted must exist in the result
  // set, though only the version field needs to be returned.
  LiveDbRethink.prototype.bulkGetSnapshot = function(requests, callback) {
    console.log('LiveDbRethink bulkGetSnapshot');
    var err = 'Not implemented';
    callback(err);
    // if (this.closed) {return callback('db already closed');}
    // var results = {};

    // var getSnapshots = function(cName, callback) {
    //   if (!isValidCName(cName)) return 'Invalid collection name';
    //   var cResult = results[cName] = {};
    //   var docNames = requests[cName];
    //   onConnect(function(err, connection) {
    //     r.table(cName)
    //       .get(docNames.id)
    //       .run(connection)
    //       .then(function(cursor) {
    //         cursor.toArray(function(err, row) {
    //           row = row && row.map(castToSnapshot);
    //           for (var i = 0; i < row.length; i++) {
    //             cResult[row[i].docName] = row[i];
    //           }
    //           callback();
    //         });
    //       })
    //       .error(function(err) {
    //         callback(err, null);
    //       });
    //     connection.close();
    //   });
    // };

    // async.each(Object.keys(requests), getSnapshots, function(err) {
    //   callback(err, err ? null : results);
    // });
  };



  // Thats it; thats the whole snapshot database API.



  // Query support API. This is optional. It allows you to run queries against the data.

  // The memory database has a really simple (probably too simple) query
  // mechanism to get all documents in the collection. The query is just the
  // collection name.

  // Ignore the query - Returns all documents in the specified index (=collection)
  LiveDbRethink.prototype.query = function(liveDb, index, query, options, callback) {
    console.log('LiveDbRethink query');
    var err = 'Not implemented';
    callback(err);
    // var collection = this.collections[index];
    // var results = [];
    // for (var docName in collection || {}) {
    //   var snapshot = collection[docName];
    //   if (!snapshot.type) continue;
    //   var result = clone(snapshot);
    //   result.docName = docName;
    //   results.push(result);
    // }
    // process.nextTick(function() {
    //   callback(null, results);
    // });
  };

  // Ignore the query - Returns all documents in the specified index (=collection)
  LiveDbRethink.prototype.queryDoc = function(liveDb, index, cName, docName, query, callback) {
    console.log('LiveDbRethink queryDoc');
    var err = 'Not implemented';
    callback(err);
    // var snapshot = this._getSnapshotSync(cName, docName);
    // var result;
    // if (snapshot && snapshot.type) {
    //   result = snapshot;
    //   result.docName = docName;
    // }
    // process.nextTick(function() {
    //   callback(null, result);
    // });
  };

  // Queries can avoid a lot of database load and CPU by querying individual
  // documents instead of the whole collection.
  LiveDbRethink.prototype.queryNeedsPollMode = function(index, query) {
    console.log('LiveDbRethink queryNeedsPollMode');
    var err = 'Not implemented';
    callback(err);
    //return false;
  };


  // ******* Oplog methods

  // Overwrite me if you want to change this behaviour.
  LiveDbRethink.prototype.getOplogCollectionName = function(cName) {
    // // Using an underscore to make it easier to see whats going in on the shell
    return cName + '_ops';
  };


  // This is used to store an operation.
  //
  // Its possible writeOp will be called multiple times with the same operation
  // (at the same version). In this case, the function can safely do nothing (or
  // overwrite the existing identical data). It MUST NOT change the version number.
  //
  // Its guaranteed that writeOp calls will be in order - that is, the database
  // will never be asked to store operation 10 before it has received operation
  // 9. It may receive operation 9 on a different server.
  //
  // opData looks like:
  // {v:version, op:... OR create:{optional data:..., type:...} OR del:true, [src:string], [seq:number], [meta:{...}]}
  //
  // callback should be called as callback(error)
  LiveDbRethink.prototype.writeOp = function(cName, docName, opData, callback) {
    assert(opData.v !== null);

    var err;
    if (err = this._check(cName)) {
      return callback(err);
    }
    var self = this;

    var data = shallowClone(opData);
    data.id = docName + ' v' + opData.v,
      data.name = docName;

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
    var ops = this.getOplogCollectionName(cName);
    onConnect(function(err, connection) {
      r.table(ops)
        .insert(data)
        .run(connection)
        .then(function(result) {
          callback(null, result);
        })
        .error(function(err) {
          callback(err, null);
        });
      connection.close();
    });
  };

  // Get the current version of the document, which is one more than the version
  // number of the last operation the database stores.
  //
  // callback should be called as callback(error, version)
  LiveDbRethink.prototype.getVersion = function(cName, docName, callback) {
    var err;
    if (err = this._check(cName)) return callback(err);
    onConnect(function(err, connection) {
      r.table(cName)
        .get(docName)
        .pluck('v')
        .run(connection)
        .then(function(result) {
          //expect a single result or a null
          if (result === []) {
            callback(null, result ? result.v : 0);
          } else {
            callback(err, result.v + 1);
          }
        })
        .error(function(err) {
          callback(err, null);
        });
      connection.close();
    });
  };

  // Get operations between [start, end) noninclusively. (Ie, the range should
  // contain start but not end).
  //
  // If end is null, this function should return all operations from start onwards.
  //
  // The operations that getOps returns don't need to have a version: field.
  // The version will be inferred from the parameters if it is missing.
  //
  // Callback should be called as callback(error, [list of ops]);
  LiveDbRethink.prototype.getOps = function(cName, docName, start, end, callback) {
    var err;
    if (err = this._check(cName)) {
      return callback(err);
    }
    var ops = this.getOplogCollectionName(cName);
    if (end === null) {
      end = 'null';
    } else {
      end.toString();
    }

    start = start.toString();

    onConnect(function(err, connection) {
      r.table(ops)
        .between([docName, start], [docName, end], {
          index: 'operations',
          left_bound: 'closed',
          right_bound: 'closed'
        })
        .run(connection)
        .then(function(cursor) {
          cursor.toArray(function(err, row) {
            // Sort the results according to version.
            var sorted = row.sort(function(a, b) {
              return a.v - b.v;
            });
            callback(null, sorted);
          });
        })
        .error(function(err) {
          callback(err, null);
        });
      connection.close();
    });
  };
}());