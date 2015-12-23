'use strict';

var util = require('util');
var R = require('ramda');
var Promise = require('bluebird');

const dbExists = (r, model) => r.dbList().contains(model.db).run();
const createDb = (r, model) => r.dbCreate(model.db).run().then(() => Promise.resolve('DB_CREATED'));
const createDbIfNot = (r, model) => dbExists(r, model).then(dbExists => (dbExists) ? Promise.resolve('DB_EXISTS') : createDb(r, model));
const tableExists = (r, model) => createDbIfNot(r, model).then(() => r.db(model.db).tableList().contains(model.table).run());
const createTable = (r, model) => createDbIfNot(r, model).then(() => r.db(model.db).tableCreate(model.table, {primaryKey: model.primaryKey}).run().then(() => Promise.resolve('TABLE_CREATED')));
const createTableIfNot = (r, model) => createDbIfNot(r, model).then(() => tableExists(r, model)).then(tableExists => (tableExists) ? Promise.resolve('TABLE_EXISTS') : createTable(r, model));
const addIndex = (r, model) => index => r.db(model.db).table(model.table).indexCreate(index);
const addIndices = (r, model) => indices => Promise.settle(R.map(query => query.run(), R.map(addIndex(r, model), indices)));
const getUndefinedFields = document => R.filter(fieldName => R.isNil(document[fieldName]), R.keys(document));
const removeUndefinedFields = document => R.isArrayLike(document)
    ? R.map(document => R.omit(getUndefinedFields(document), document), document)
    : R.omit(getUndefinedFields(document), document);

const getTransformations = R.curry((value, fieldNames) => R.pipe(R.map(fieldName => [fieldName, R.always(value)]), R.fromPairs)(fieldNames));
const assocAll = R.curry((fieldNames, value, doc) => R.evolve(getTransformations(value, fieldNames), doc));
const setUndefinedFieldsNull = R.ifElse(
        R.isArrayLike,
        R.map(doc => getUndefinedFields(doc)),
        R.pipe(getUndefinedFields,assocAll(R.__, null))
    );

const _init = (r, model) => ({
    createDbAndTable: () => (model.db && model.table)
            ? createTableIfNot(r, model).then(() => addIndices(r, model)(model.indices || []))
            : Promise.reject('INVALID_MODEL'),
    replace: document => {
        if (!document || 0 === document.length) {
            return Promise.reject(`***ALERTS_API*** _replace() no connection made to rethink to _replace - the document is empty`);
        } else {
            return r.db(model.db).table(model.table).insert(removeUndefinedFields(document), {conflict: 'replace', returnChanges: true}).run();
        }
    },
    update: document => {
        if (!document || 0 === document.length) {
            return Promise.reject('***ALERTS_API***  _replace() no connection made to rethink to _update - the document is empty');
        } else {
            return r.db(model.db).table(model.table).insert(setUndefinedFieldsNull(document), {conflict: 'update', returnChanges: true}).run();
        }
    },
    findById: id => r.db(model.db).table(model.table).get(id).run(),
    findAll: () => r.db(model.db).table(model.table).run(),
    findByIds: ids => r.db(model.db).table(model.table).getAll(r.args(ids)).run().then(cursor => cursor.toArray()),
    subscribeChanges: () => r.db(model.db).table(model.table).changes().run(),
    findMaxBy: field => r.db(model.db).table(model.table).max(field).pluck(field).run(),
    findByFilter: filter => r.db(model.db).table(model.table).filter(filter).run().then(cursor => cursor.toArray()),
    getRDash: r
});

module.exports = model => {
    return _init(require('rethinkdbdash')({
        max: model.poolMax || 100,
        port: model.port,
        host: model.host,
        cursor: model.useCursors,
    }), R.clone(model));
};