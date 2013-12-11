var async = require('async');
var assert = require('assert');
var ObjectId = require('mongodb').ObjectID;
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var Promise = require('./promise');

var noop = function() {};

/**
 Mongodb _id values are not totally ordered and it doesn't correspond to the $natural order.
 Multiple writers can together produce out of order _id values, because of process ids and/or
 clock drift.
 To find all documents that come after a specific document a, if you use {_id: {$gt: a._id}}
 you might miss some.
 Therefore, you need to use a madeup _id that is sure to be before a._id and skip over the
 found documents until you reach a.

 To "fix" the id, we create one that is several seconds older to account for clock drift.
**/
function searchId(_id) {
    var seconds = parseInt(_id.toHexString().slice(0, 8), 16);
    return ObjectId.createFromTime(seconds - 16); // 16 seconds before _id was created
}

/**
 * Channel constructor.
 *
 * @param {Connection} connection
 * @param {String} [name] optional channel/collection name, default is 'mubsub'
 * @param {Object} [options] optional options
 *   - `size` max size of the collection in bytes, default is 5mb
 *   - `max` max amount of documents in the collection
 *   - `retryInterval` time in ms to wait if no docs found, default is 200ms
 *   - `recreate` recreate the tailable cursor on error, default is true
 * @api public
 */
function Channel(connection, name, options, backlogCount) {
    options || (options = {});
    options.capped = true;
    // In mongo v <= 2.2 index for _id is not done by default
    options.autoIndexId = true;
    options.size || (options.size = 1024 * 1024 * 5);
    options.retryInterval || (options.retryInterval = 200);
    options.recreate != null || (options.recreate = true);
    this.options = options;
    this.connection = connection;
    this.closed = false;
    this.listening = null;
    this.name = name || 'mubsub';
    this.create().listen(null, backlogCount);
}

module.exports = Channel;
util.inherits(Channel, EventEmitter);

/**
 * Close the channel.
 *
 * @return {Channel} this
 * @api public
 */
Channel.prototype.close = function() {
    this.closed = true;

    return this;
};

/**
 * Publish an event.
 *
 * @param {String} event
 * @param {Object} [message]
 * @param {Function} [callback]
 * @return {Channel} this
 * @api public
 */
Channel.prototype.publish = function(event, message, callback) {
    var options = callback ? {safe: true} : {};
    callback || (callback = noop);

    this.ready(function(collection) {
        collection.insert({event: event, message: message}, options, function(err, docs) {
            if (err) return callback(err);
            callback(null, docs[0]);
        });
    });

    return this;
};

/**
 * Subscribe an event.
 *
 * @param {String} [event] if no event passed - all events are subscribed.
 * @param {Function} callback
 * @return {Object} unsubscribe function
 * @api public
 */
Channel.prototype.subscribe = function(event, callback) {
    var self = this;

    if (typeof event == 'function') {
        callback = event;
        event = 'message';
    }

    this.on(event, callback);

    return {
        unsubscribe: function() {
            self.removeListener(event, callback);
        }
    };
};

/**
 * Create a channel collection.
 *
 * @return {Channel} this
 * @api private
 */
Channel.prototype.create = function() {
    var self = this;

    function create() {
        self.connection.db.createCollection(
            self.name,
            self.options,
            self.collection.resolve.bind(self.collection)
        );
    }

    this.collection = new Promise();
    this.connection.db ? create() : this.connection.once('connect', create);

    return this;
};

/**
 * Create a listener which will emit events for subscribers.
 * It will listen to any document with event property.
 *
 * @param {Object} [latest] latest document to start listening from
 * @return {Channel} this
 * @api private
 */
Channel.prototype.listen = function(latest, backlogCount) {
    var self = this;

    function internalListen(latest, firstBacklog, collection) {
        var state = 'preroll' // other values: 'backlog', 'live'
        var currentBacklog = firstBacklog;
        var startId = searchId(firstBacklog ? firstBacklog._id : latest._id);
        var prerollEndId = firstBacklog ? firstBacklog._id : latest._id;
        var cursor = collection
                .find(
                    {_id: {$gt: startId}},
                    {tailable: true, awaitdata: true, numberOfRetries: -1, tailableRetryInterval: self.options.retryInterval}
                )
                .sort({$natural: 1});

        function preroll(doc) {
            // There is no document only if the cursor is closed by accident.
            // F.e. if collection was dropped or connection died.
            if (doc) {
                if (doc._id.equals(prerollEndId)) {
                    if (firstBacklog) {
                        state = 'backlog';
                    } else {
                        state = 'live';
                        self.emit('live');
                    }
                }
            }

            if (cursor.isClosed()) {
                self.emit('error', new Error('Mubsub: broken cursor.'));
                if (self.options.recreate) {
                    process.nextTick(function() {
                        self.listen(latest, backlogCount);
                    });
                }
            } else {
                process.nextTick(more);
            }
        }

        function backlog(doc) {
            // There is no document only if the cursor is closed by accident.
            // F.e. if collection was dropped or connection died.
            if (doc) {
                currentBacklog = doc;

                if (doc.event) {
                    self.emit('backlog', doc.message);
                }

                if (doc._id.equals(latest._id)) {
                    state = 'live';
                    self.emit('live');
                }
            }

            if (cursor.isClosed()) {
                self.emit('error', new Error('Mubsub: broken cursor.'));
                if (self.options.recreate) {
                    process.nextTick(function() {
                        internalListen(latest, currentBacklog, collection);
                    });
                }
            } else {
                process.nextTick(more);
            }
        }

        function live(doc) {
            // There is no document only if the cursor is closed by accident.
            // F.e. if collection was dropped or connection died.
            if (doc) {
                latest = doc;

                if (doc.event) {
                    self.emit(doc.event, doc.message);
                    self.emit('message', doc.message);
                }
                self.emit('document', doc);
            }

            if (cursor.isClosed()) {
                self.emit('error', new Error('Mubsub: broken cursor.'));
                if (self.options.recreate) {
                    process.nextTick(function() {
                        self.collection.then(self.handle(true, function (collection) {
                            internalListen(latest, null, collection);
                        }));
                    });
                }
            } else {
                process.nextTick(more);
            }
        }

        var next = self.handle(function (doc) {
                switch(state) {
                case 'preroll':
                    preroll(doc);
                    break;
                case 'backlog':
                    backlog(doc);
                    break;
                case 'live':
                    live(doc);
                    break;
                default:
                    assert(false, 'illegal state in Channel.prototype.listen: ' + state);
                }
            });

        var more = function() {
            cursor.nextObject(next);
        };

        more();
        self.listening = collection;
        self.emit('ready', collection);
    };

    this.latest(latest, backlogCount, this.handle(true, function(latest, firstBacklog, collection) {
        internalListen(latest, firstBacklog, collection);
    }));

    return this;
};

/**
 * Get the latest document from the collection. Insert a dummy object in case
 * the collection is empty, because otherwise we don't get a tailable cursor
 * and need to poll in a loop.
 *
 * @param {Object} [latest] latest known document
 * @param {Function} callback (err, latestDoc, firstBacklog, collection)
 * @return {Channel} this
 * @api private
 */
Channel.prototype.latest = function(latest, backlogCount, callback) {
    var self = this;

    this.collection.then(function(err, collection) {
        if (err) return callback(err);

        async.waterfall([
            function (cb) {
                // find latest document
                collection
                    .find(latest ? {_id: latest._id} : null)
                    .sort({$natural: -1})
                    .limit(1)
                    .nextObject(cb);
            },
            function (latest, cb) {
                if (latest) {
                    // find first backlog document
                    // I know this is not efficient
                    if (backlogCount) {
                        collection
                            .find({_id: {$lt: latest._id}}, {_id: 1})
                            .sort({$natural: -1})
                            .limit(backlogCount)
                            .toArray(function (err, docs) {
                                // the last one in the array is the oldest or first backlog doc
                                cb(err, latest, docs && docs.length && docs[docs.length - 1]);
                            });
                    } else {
                        cb(null, latest, null);
                    }
                } else {
                    // insert a doc
                    collection.insert({dummy: true}, {safe: true}, function(err, docs) {
                        cb(err, docs[0], null);
                    });
                }
            }
        ], function (err, latest, firstBacklog) {
            callback(err, latest, firstBacklog, collection);
        });
    });

    return this;
};

/**
 * Return a function which will handle errors and consider channel and connection
 * state.
 *
 * @param {Boolean} [exit] if error happens and exit is true, callback will not be called
 * @param {Function} callback
 * @return {Function}
 * @api private
 */
Channel.prototype.handle = function(exit, callback) {
    var self = this;

    if (typeof exit === 'function') {
        callback = exit;
        exit = null;
    }

    return function() {
        if (self.closed || self.connection.destroyed) return;

        var args = [].slice.call(arguments);
        var err = args.shift();

        if (err) self.emit('error', err);
        if (err && exit) return;

        callback.apply(self, args);
    };
};

/**
 * Call back if collection is ready for publishing.
 *
 * @param {Function} callback
 * @return {Channel} this
 * @api private
 */
Channel.prototype.ready = function(callback) {
    if (this.listening) {
        callback(this.listening);
    } else {
        this.once('ready', callback);
    }

    return this;
};
