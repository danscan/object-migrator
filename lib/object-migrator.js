var _ = require('underscore'),
    Q = require('q'),
    async = require('async'),
    migratePath;

migratePaths = function migratePath(paths) {
  paths = _.map(paths, function(pathMap, sourcePath) {
    return _.extend(pathMap, { sourcePath: sourcePath });
  });

  return function migrateDocument(document, next) {
    console.log('Migrating paths for document %s.', document._id);

    async.each(paths, function migratePath(path, next) {
      var emptyMigrator = function emptyMigrator(v, d, cb) { cb(null, v); };

      path.validator = path.validator || function emptyValidator(v,d,cb) { cb(null, true); };
      path.destinationPath = path.destinationPath || path.sourcePath;

      return Q.nfcall(path.validator, document[path.sourcePath], document)
        .then(function pathValidatorCallback(valid) {
          if (! valid) {
            console.log('Path "%s" value is not valid for migration. Skipping...', path.sourcePath);
            return Q.nfcall(emptyMigrator, sourcePathValue, document);
          } else {
            console.log('Migrate: %s -> %s', path.sourcePath, path.destinationPath);
            return Q.ninvoke(path, 'migrator', document[path.sourcePath], document);
          }
        })
        .then(function migratorSuccessCallback(value) {
          document.set(path.destinationPath, value);

          return Q.ninvoke(document, 'save');
        })
        .nodeify(next);
    }, next);
  };
};

module.exports = function ObjectMigratorConstructor(paths) {
  this.paths = paths;

  return function ObjectMigrator(Model, callback) {
    Q.ninvoke(Model, 'find', {})
      .then(function modelFindSuccessCallback(documents) {
        return Q.ninvoke(async, 'eachLimit', documents, 5000, migratePaths(paths));
      })
      .nodeify(callback);
  };
};


