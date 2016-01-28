'use strict';

var util = require('util');
var R = require('ramda');
var Promise = require('bluebird');

var dbExists = function dbExists(r, model) {
	    return r.dbList().contains(model.db).run();
};
var createDb = function createDb(r, model) {
	    return r.dbCreate(model.db).run().then(function () {
		            return Promise.resolve('DB_CREATED');
			        });
};
var createDbIfNot = function createDbIfNot(r, model) {
	    return dbExists(r, model).then(function (dbExists) {
		            return dbExists ? Promise.resolve('DB_EXISTS') : createDb(r, model);
			        });
};
var tableExists = function tableExists(r, model) {
	    return createDbIfNot(r, model).then(function () {
		            return r.db(model.db).tableList().contains(model.table).run();
			        });
};
var createTable = function createTable(r, model) {
	    return createDbIfNot(r, model).then(function () {
		            return r.db(model.db).tableCreate(model.table, { primaryKey: model.primaryKey }).run().then(function () {
				                return Promise.resolve('TABLE_CREATED');
						        });
			        });
};
var createTableIfNot = function createTableIfNot(r, model) {
	    return createDbIfNot(r, model).then(function () {
		            return tableExists(r, model);
			        }).then(function (tableExists) {
		            return tableExists ? Promise.resolve('TABLE_EXISTS') : createTable(r, model);
			        });
};
var addIndex = function addIndex(r, model) {
	    return function (index) {
		            return r.db(model.db).table(model.table).indexCreate(index);
			        };
};
var addIndices = function addIndices(r, model) {
	    return function (indices) {
		            return Promise.settle(R.map(function (query) {
				                return query.run();
						        }, R.map(addIndex(r, model), indices)));
			        };
};
var getUndefinedFields = function getUndefinedFields(document) {
	    return R.filter(function (fieldName) {
		            return R.isNil(document[fieldName]);
			        }, R.keys(document));
};
var removeUndefinedFields = function removeUndefinedFields(document) {
	    return R.isArrayLike(document) ? R.map(function (document) {
		            return R.omit(getUndefinedFields(document), document);
			        }, document) : R.omit(getUndefinedFields(document), document);
};

var getTransformations = R.curry(function (value, fieldNames) {
	    return R.pipe(R.map(function (fieldName) {
		            return [fieldName, R.always(value)];
			        }), R.fromPairs)(fieldNames);
});

var assocAll = R.curry((fieldNames, value, doc) => R.evolve(getTransformations(value, fieldNames), doc));
var setFieldsNull = doc => assocAll(getUndefinedFields(doc), null, doc);


var setUndefinedFieldsNull = R.ifElse(
		    R.isArrayLike,
		        R.map(setFieldsNull),
			    setFieldsNull);

var _init = function _init(r, model) {
	    return {
		            createDbAndTable: function createDbAndTable() {
						                  return model.db && model.table ? createTableIfNot(r, model).then(function () {
									                  return addIndices(r, model)(model.indices || []);
											              }) : Promise.reject('INVALID_MODEL');
								          },
				            replace: function replace(document) {
							                 if (!document || 0 === document.length) {
										                 return Promise.reject('***ALERTS_API*** _replace() no connection made to rethink to _replace - the document is empty');
												             } else {
														                     return r.db(model.db).table(model.table).insert(removeUndefinedFields(document), { conflict: 'replace', returnChanges: true }).run();
																                 }
									         },
					            update: function update(document) {
								                if (!document || 0 === document.length) {
											                return Promise.reject('***ALERTS_API***  _replace() no connection made to rethink to _update - the document is empty');
													            } else {
															                    return r.db(model.db).table(model.table).insert(setUndefinedFieldsNull(document), { conflict: 'update', returnChanges: true }).run();
																	                }
										        },
						            findById: function findById(id) {
									                  return r.db(model.db).table(model.table).get(id).run();
											          },
							            findAll: function findAll() {
										                 return r.db(model.db).table(model.table).run();
												         },
								            findByIds: function findByIds(ids) {
											                   return r.db(model.db).table(model.table).getAll(r.args(ids)).run().then(function (cursor) {
														                   return cursor.toArray();
																               });
													           },
									            subscribeChanges: function subscribeChanges() {
													                  return r.db(model.db).table(model.table).changes().run();
															          },
										            findMaxBy: function findMaxBy(field) {
													                   return r.db(model.db).table(model.table).max(field).pluck(field).run();
															           },
											            findByFilter: function findByFilter(filter) {
															              return r.db(model.db).table(model.table).filter(filter).run().then(function (cursor) {
																	                      return cursor.toArray();
																			                  });
																              },
												            getRDash: r
														        };
};

module.exports = function (model) {
	    return _init(require('rethinkdbdash')({
		            max: model.poolMax || 100,
		           port: model.port,
		           host: model.host,
		           cursor: model.useCursors
		        }), R.clone(model));
};
