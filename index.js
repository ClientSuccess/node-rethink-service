var util = require('util');
var R = require('ramda');
var Promise = require('bluebird');
var r;

function _dbExists(model) {
    return r.dbList().contains(model.db).run();
}

function _createDbIfNot(model) {
    return _dbExists(model).then(dbExists => (dbExists) ? Promise.resolve('DB_EXISTS') : _createDb(model));
}

function _tableExists(model) {
    return _createDbIfNot(model).then(() => r.db(model.db).tableList().contains(model.table).run());
}

function _createDb(model) {
    return r.dbCreate(model.db).run().then(() => Promise.resolve('DB_CREATED'));
}

function _createTable(model) {
    return _createDbIfNot(model)
        .then(() => r.db(model.db).tableCreate(model.table, {primaryKey: model.primaryKey}).run()
            .then(() => Promise.resolve('TABLE_CREATED')));
}

function _createTableIfNot(model) {
    return _createDbIfNot(model)
        .then(() => _tableExists(model))
        .then(tableExists => (tableExists)
            ? Promise.resolve('TABLE_EXISTS')
            : _createTable(model));
}

function _createDbAndTable(model) {
    return () => (model.db && model.table)
        ? _createTableIfNot(model)
        .then(() => _addIndices(model)(model.indices || []))
        : Promise.reject('INVALID_MODEL');
}

function _addIndices(model) {
    return indices => Promise.settle(R.map(query => query.run(), R.map(_addIndex(model), indices)));
}

function _addIndex(model) {
    return index => r.db(model.db).table(model.table).indexCreate(index);
}

function _replace(model) {
    return (document => {
        if (!document || 0 === document.length) {
            return Promise.reject(`_replace - the document is empty ${util.inspect(document)}`);
        } else {
            return r.db(model.db).table(model.table).insert(_removeUndefinedFields(document), {conflict: 'replace', returnChanges: true}).run();
        }
    });
}

function _update(model) {
    return (document => {
        if (!document || 0 === document.length) {
            return Promise.reject('_update - the document is empty');
        } else {
            return r.db(model.db).table(model.table).insert(_setUndefinedFieldsNull(document), {conflict: 'update', returnChanges: true}).run();
        }
    });
}

function _findById(model) {
    return id => r.db(model.db).table(model.table).get(id).run();
}

function _findByIds(model) {
    return ids => r.db(model.db).table(model.table).getAll(r.args(ids)).run()
        .then(cursor => cursor.toArray());
}

function _subscribeChanges(model) {
    return () => r.db(model.db).table(model.table).changes().run();
}

function _findMaxBy(model) {
    return field => r.db(model.db).table(model.table).max(field).pluck(field).run();
}

function _findByFilter(model) {
    return filter => r.db(model.db).table(model.table).filter(filter).run()
        .then(cursor => cursor.toArray());
}

function _getUndefinedFields(document) {
    return R.filter(fieldName => R.isNil(document[fieldName]), R.keys(document));
}

function _removeUndefinedFields(document) {

    return R.isArrayLike(document)
        ? R.map(document => R.omit(_getUndefinedFields(document), document), document)
        : R.omit(_getUndefinedFields(document), document);
}

function _setUndefinedFieldsNull(document) {
    let doOmit = doc => R.tail(R.map(field => R.assoc(field, null, doc), _getUndefinedFields(doc)));

    return R.isArrayLike(document)
        ? R.map(doOmit, document)
        : doOmit(document);
}

function _getRDash() {
    return r;
}

module.exports = model => {
    r = require('rethinkdbdash')({
        max: 100,
        port: model.port,
        host: model.host,
        authKey: model.authKey,
        cursor: true,
        ssl: {ca: model.cert}
    });

    return {
        createDbAndTable: _createDbAndTable(R.clone(model)),
        subscribeChanges: _subscribeChanges(R.clone(model)),
        replace: _replace(R.clone(model)),
        update: _update(R.clone(model)),
        findById: _findById(R.clone(model)),
        findByIds: _findByIds(R.clone(model)),
        findMaxBy: _findMaxBy(R.clone(model)),
        findByFilter: _findByFilter(R.clone(model)),
        getRDash: _getRDash
    };
};