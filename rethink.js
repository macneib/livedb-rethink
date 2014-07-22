'use strict';

var _require = require;
var r = require('rethinkdb');
var mongoskin = _require('mongoskin');
var Promise = require('bluebird');
var assert = require('assert');
var async = require('async');
var util = require('util');

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

var dbConfig = {
  host: 'localhost',
  port: 28015,
  authKey: '',
  db: 'test',
  tables: {
    'collection': 'id'
  }
};

//console.log('rethink connection', r);
/* There are two ways to instantiate a livedb-rethink wrapper.
 *
 * 1. The simplest way is to just invoke the module and pass in your rethinkdbdash
 * arguments as arguments to the module function. For example:
 *
 * TODO
 *
 * 2. If you already have a mongoskin instance that you want to use, you can
 * just pass it into livedb-mongo:
 *
 * TODO
 */

exports = module.exports = function() {
//  if (util.isArray(r) || typeof r !== 'object') {
//    //r = r.db.apply(r.db, arguments);
//  }
  return new LiveDbRethink(dbConfig);
};

// Deprecated. Don't use directly.
exports.LiveDbRethink = LiveDbRethink;

// r is a rethinkdbdash client. Create with:
// var r = require('rethinkdbdash')();
function LiveDbRethink(options) {
  // TODO: RETHINK .connect
  this.r = r;
  this.closed = false;

  if (options && options.rethinkPoll) {
    this.rethinkPoll = options.rethinkPoll;
  }

  r.connect({host: dbConfig.host, port: dbConfig.port }, function (err, connection) {
    if(err) {
      console.log('DB CONNECTION FAILED: ', err);
    } else {
      console.log('CONNECTED TO DB SUCCESSFULLY');

      for (var table in dbConfig.tables) {
        r.db(dbConfig.db).tableCreate(table, {
          primaryKey: dbConfig.tables[table]
        }).run(connection, function(err, result) {
          if(err) {
            console.log('[TABLE CREATE] - ' + table + ' FAILURE');
          } else {
            console.log('[TABLE CREATE] - ' + table + ' SUCCESS');
          }
        });
      }
    }
  });


  /*
  // Run a query
  var query = r.table('collection').get('test').pluck({id: 'test'}).run(function(err, result) {
    if(err) {
      console.log('getSnapshot error ', err);
    } else {
      console.log('getSnapshot', result);  
    }
  });
  */


  // The getVersion() and getOps() methods depend on a collectionname_ops
  // collection, and that collection should have an index on the operations
  // stored there. I could ask people to make these indexes themselves, but
  // even I forgot on some of my collections, so the rethink driver will just do
  // it manually. This approach will leak memory relative to the number of
  // collections you have, but if you've got thousands of mongo collections
  // you're probably doing something wrong.

  // map from collection name -> true for op collections we've ensureIndex'ed
  this.opIndexes = {};

  // Allow $while queries. They're a security hole because you can run
  // server-side javascript.
  this.allowWhereQuery = options ? (options.allowWhereQuery || false) : false;
}

LiveDbRethink.prototype.close = function(callback) {
  if (this.closed) return callback('db already closed');

  //this.mongo.close(callback);

  // TODO: RETHINK .close MAYBE THIS SHOLD NOT BE HERE.
  r.getPool().drain(callback);
  //this.r.close(callback);
  this.closed = true;
};

function isValidCName(cName) {
  return !(/_ops$/.test(cName)) && cName !== 'system';
}

LiveDbRethink.prototype._check = function(cName) {
  if (this.closed) return 'db already closed';
  if (!isValidCName(cName)) return 'Invalid collection name';
};

// **** Snapshot methods

LiveDbRethink.prototype.getSnapshot = function(cName, docName, callback) {
  var err;
  if (err = this._check(cName)) return callback(err);

  /*Rethink code*/

  //var result = r.table('collection').get(cName).pluck({id: docName}).run();
  //console.log('getSnapshot result', result);

  // Run a query
  var query = r.table('collection').get(cName).pluck({id: docName}).then(function(result) {
    // handle result
    console.log('getSnapshot', result);
    callback(err, castToSnapshot(result));
  }).error(function(err) {
    // handle error
     console.log('getSnapshot error ', err);
  }).catch(function(err) {
    // handle exception
    console.log('getSnapshot exception ', err);
  });



  // TODO: RETHINK .get limit(-1)
  // this.mongo.collection(cName).findOne({_id: docName}, function(err, doc) {
  //   callback(err, castToSnapshot(doc));
  // });

};

// Variant on getSnapshot (above) which projects the returned document
LiveDbRethink.prototype.getSnapshotProjected = function(cName, docName, fields, callback) {
  var err;
  if (err = this._check(cName)) return callback(err);

  // This code depends on the document being stored in the efficient way (which is to say, we've
  // promoted all fields in mongo). This will only work properly for json documents - which happen
  // to be the only types that we really want projections for.
  var projection = projectionFromFields(fields);



  /*Rethink code*/
  // TODO what the heck does projection do here?

  //var result = r.table('collection').get(cName).pluck({id: docName}).run();
  //console.log('getSnapshot result', result);
  //doc = result;
  //callback(err, castToSnapshot(doc));


  // var run = Promise.coroutine(function () {
  //     var doc;

  //     try{
  //         doc = r.table('collection').get(cName).pluck({id: docName}).run();
  //         console.log(JSON.stringify(result, null, 2));
  //         callback(err, castToSnapshot(doc));
  //     }
  //     catch(e) {
  //         console.log(e);
  //     }
  // })();


  // TODO: RETHINK .get limit(-1)
  // this.mongo.collection(cName).findOne({_id: docName}, projection, function(err, doc) {
  //   callback(err, castToSnapshot(doc));
  // });
};

LiveDbRethink.prototype.bulkGetSnapshot = function(requests, callback) {
  if (this.closed) return callback('db already closed');

  var r = this.r;
  var results = {};

  var getSnapshots = function(cName, callback) {
    if (!isValidCName(cName)) return 'Invalid collection name';

    var cResult = results[cName] = {};

    var docNames = requests[cName];
    /*Rethink code*/
    // TODO what the heck does projection do here?

    //var result = r.table('collection').getAll(cName).pluck({id: docName}).run();

    // var run = Promise.coroutine(function () {
    //     var doc;

    //     try{
    //         doc = r.table('collection').getAll(cName).pluck({id: docName}).run();
    //         console.log(JSON.stringify(result, null, 2));
    //         callback(err, castToSnapshot(doc));
    //     }
    //     catch(e) {
    //         console.log(e);
    //     }
    // })();


    // TODO: RETHINK .get
    // mongo.collection(cName).find({_id:{$in:docNames}}).toArray(function(err, data) {
    //   if (err) return callback(err);
    //   data = data && data.map(castToSnapshot);

    //   for (var i = 0; i < data.length; i++) {
    //     cResult[data[i].docName] = data[i];
    //   }
    callback();
    // });
  };

  async.each(Object.keys(requests), getSnapshots, function(err) {
    callback(err, err ? null : results);
  });
};

LiveDbRethink.prototype.writeSnapshot = function(cName, docName, data, callback) {
  var err;
  if (err = this._check(cName)) return callback(err);
  var doc = castToDoc(docName, data);
  //r.table("???").update({_id: docName}).run(conn, callback)
  this.mongo.collection(cName).update({
    _id: docName
  }, doc, {
    upsert: true
  }, callback);
};


// ******* Oplog methods

// Overwrite me if you want to change this behaviour.
LiveDbRethink.prototype.getOplogCollectionName = function(cName) {
  // Using an underscore to make it easier to see whats going in on the shell
  return cName + '_ops';
};

// Get and return the op collection from mongo, ensuring it has the op index.
LiveDbRethink.prototype._opCollection = function(cName) {
  /*var collection = this.mongo.collection(this.getOplogCollectionName(cName));

  if (!this.opIndexes[cName]) {
    collection.ensureIndex({name: 1, v: 1}, true, function(error, name) {
      if (error) console.warn('Warning: Could not create index for op collection:', error.stack || error);
    });

    this.opIndexes[cName] = true;
  }

  return collection;*/
};

LiveDbRethink.prototype.writeOp = function(cName, docName, opData, callback) {
  assert(opData.v != null);

  var err;
  if (err = this._check(cName)) return callback(err);
  var self = this;

  var data = shallowClone(opData);
  data._id = docName + ' v' + opData.v,
  data.name = docName;
  // TODO: RETHINK insert?
  this._opCollection(cName).save(data, callback);
};

LiveDbRethink.prototype.getVersion = function(cName, docName, callback) {
  var err;
  if (err = this._check(cName)) return callback(err);
  // TODO: RETHINK .get limit(-1)
  this._opCollection(cName).findOne({
    name: docName
  }, {
    sort: {
      v: -1
    }
  }, function(err, data) {
    if (err) return callback(err);

    if (data == null) {
      callback(null, 0);
    } else {
      callback(err, data.v + 1);
    }
  });
};

LiveDbRethink.prototype.getOps = function(cName, docName, start, end, callback) {
  var err;
  if (err = this._check(cName)) return callback(err);

  var query = end == null ? {
    $gte: start
  } : {
    $gte: start,
    $lt: end
  };
  // MAYBE TODO: RETHINK .get ?
  /*  this._opCollection(cName).find({name:docName, v:query}, {sort:{v:1}}).toArray(function(err, data) {
    if (err) return callback(err);

    for (var i = 0; i < data.length; i++) {
      // Strip out _id in the results
      delete data[i]._id;
      delete data[i].name;
    }
    callback(null, data);
  });*/

};


// ***** Query methods

// Internal method to actually run the query.
LiveDbRethink.prototype._query = function(mongo, cName, query, fields, callback) {
  // For count queries, don't run the find() at all. We also ignore the projection, since its not
  // relevant.
  if (query.$count) {
    delete query.$count;
    // TODO RETHINK .count()
    // mongoDB Counts the number of documents in a collection. Returns a document that contains this count and as well as the command status. count has the following form:
    // MAYBE r.table('users').count().run(conn, callback)
    // http://rethinkdb.com/api/javascript/count/
    /*    mongo.collection(cName).count(query.$query || {}, function(err, count) {
      if (err) return callback(err);

      // This API is kind of awful. FIXME in livedb.
      callback(err, {results:[], extra:count});
    });*/
  } else {
    var cursorMethods = extractCursorMethods(query);

    // Weirdly, if the requested projection is empty, we send everything.
    var projection = fields ? projectionFromFields(fields) : {};
    // MAYBE TODO: RETHINK .get ?
    /*    mongo.collection(cName).find(query, projection, function(err, cursor) {
      if (err) return callback(err);

      for (var i = 0; i < cursorMethods.length; i++) {
        var item = cursorMethods[i];
        var method = item[0];
        var arg = item[1];
        cursor[method](arg);
      }

      cursor.toArray(function(err, results) {
        results = results && results.map(castToSnapshot);
        callback(err, results);
      });
    });*/
  }

};

LiveDbRethink.prototype.query = function(livedb, cName, inputQuery, opts, callback) {
  // Regular queries are just a special case of queryProjected, but with fields=null (which livedb
  // will never pass naturally).
  this.queryProjected(livedb, cName, null, inputQuery, opts, callback);
};

LiveDbRethink.prototype.queryProjected = function(livedb, cName, fields, inputQuery, opts, callback) {
  var err;
  if (err = this._check(cName)) return callback(err);

  // To support livedb <=0.2.8
  if (typeof opts === 'function') {
    callback = opts;
    opts = {};
  }

  var query = normalizeQuery(inputQuery);
  var err = this.checkQuery(query);
  if (err) return callback(err);

  // Use this.rethinkPoll if its a polling query.
  if (opts.mode === 'poll' && this.rethinkPoll) {
    var self = this;
    // This timeout is a dodgy hack to work around race conditions replicating the
    // data out to the polling target replica.
    setTimeout(function() {
      if (self.closed) return callback('db already closed');
      self._query(self.rethinkPoll, cName, query, fields, callback);
    }, 300);
  } else {
    this._query(this.mongo, cName, query, fields, callback);
  }
};

LiveDbRethink.prototype.queryDocProjected = function(livedb, index, cName, docName, fields, inputQuery, callback) {
  var err;
  if (err = this._check(cName)) return callback(err);
  var query = normalizeQuery(inputQuery);
  if (err = this.checkQuery(query)) return callback(err);

  // Run the query against a particular mongo document by adding an _id filter
  var queryId = query.$query._id;
  if (queryId) {
    delete query.$query._id;
    query.$query.$and = [{
      _id: docName
    }, {
      _id: queryId
    }];
  } else {
    query.$query._id = docName;
  }

  var projection = fields ? projectionFromFields(fields) : {};

  function cb(err, doc) {
    callback(err, castToSnapshot(doc));
  }

  if (this.rethinkPoll) {
    var self = this;
    // Blah vomit - same dodgy hack as in queryProjected above.
    setTimeout(function() {
      if (self.closed) return callback('db already closed');
      // TODO: RETHINK .get limit(-1)
      self.rethinkPoll.collection(cName).findOne(query, projection, cb);
    }, 300);
  } else {
    // TODO: RETHINK .get limit(-1)
    //this.mongo.collection(cName).findOne(query, projection, cb);
  }
};

LiveDbRethink.prototype.queryDoc = function(livedb, index, cName, docName, inputQuery, callback) {
  this.queryDocProjected(livedb, index, cName, docName, null, inputQuery, callback);
};

// Test whether an operation will make the document its applied to match the
// specified query. This function doesn't really have enough information to know
// in all cases, but if we can determine whether a query matches based on just
// the operation, it saves doing extra DB calls.
//
// currentStatus is true or false depending on whether the query currently
// matches. return true or false if it knows, or null if the function doesn't
// have enough information to tell.
LiveDbRethink.prototype.willOpMakeDocMatchQuery = function(currentStatus, query, op) {
  return null;
};

// Does the query need to be rerun against the database with every edit?
LiveDbRethink.prototype.queryNeedsPollMode = function(index, query) {
  return query.hasOwnProperty('$orderby') ||
    query.hasOwnProperty('$limit') ||
    query.hasOwnProperty('$skip') ||
    query.hasOwnProperty('$count');
};


// Utility methods

// Return error string on error. Query should already be normalized with
// normalizeQuery below.
LiveDbRethink.prototype.checkQuery = function(query) {
  if (!this.allowWhereQuery && query.$query.$where != null)
    return "Illegal $where query";
};

function extractCursorMethods(query) {
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
  if (!query.$query._type) query.$query._type = {
    $ne: null
  };

  return query;
}

function castToDoc(docName, data) {
  var doc = (
      typeof data.data === 'object' &&
      data.data !== null &&
      !Array.isArray(data.data)
    ) ?
    shallowClone(data.data) : {
      _data: (data.data === void 0) ? null : data.data
    };
  doc._type = data.type || null;
  doc._v = data.v;
  doc._m = data.m;
  doc._id = docName;
  return doc;
}

function castToSnapshot(doc) {
  if (!doc) return;
  var type = doc._type;
  var v = doc._v;
  var docName = doc._id;
  var data = doc._data;
  var meta = doc._m;
  if (data === void 0) {
    doc = shallowClone(doc);
    delete doc._type;
    delete doc._v;
    delete doc._id;
    delete doc._m;
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
  var projection = {};
  for (var k in fields) {
    projection[k] = 1;
  }
  projection._v = 1;
  projection._type = 1;
  projection._m = 1;

  return projection;
}