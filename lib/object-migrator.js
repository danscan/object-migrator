var _ = require('underscore'),
    Q = require('q'),
    async = require('async'),
    debug = require('debug')('OBJECT_MIGRATOR'),
    migratePaths;

module.exports = function ObjectMigratorConstructor(pathMaps) {

  // Set each pathMap's sourcePath value
  pathMaps = _.map(pathMaps, function(pathMap, sourcePath) {
    return _.extend(pathMap, _.extend(pathMap, { sourcePath: sourcePath }));
  });
  this.pathMaps = pathMaps;

  /**
   * Return ObjectMigrator function that 
   * takes a (Mongoose) model and a completion callback
   */
  return function ObjectMigrator(Model, callback) {

    /**
     * Find all documents via provided model
     */
    return Q.ninvoke(Model, 'find', {})
      .then(function modelFindSuccessCallback(documents) {

        /**
         * Migrate all pathMaps for each document
         */
        return Q.ninvoke(async, 'eachLimit', documents, 5000, function migrateDocument(document, next) {
          debug('About to migrate pathMaps for document %s.', document._id);

          /**
           * Migrate each pathMap for this document
           */
          return Q.ninvoke(async, 'eachLimit', pathMaps, 5000, function migrateDocumentPath(pathMap, next) {
            var emptyMigrator = function emptyMigrator(v,d,cb){cb(null,d[pathMap.destinationPath]);};

            pathMap.validator = pathMap.validator || function emptyValidator(v,d,cb){cb(null,true);},
            pathMap.destinationPath = pathMap.destinationPath || pathMap.sourcePath;
                
            return Q.nfcall(pathMap.validator, document[pathMap.sourcePath], document)
              .then(function pathValidatorCallback(valid) {
                if (! valid) {
                  debug('Path "%s" value is not valid for migration. Skipping...', pathMap.sourcePath);
                  return Q.nfcall(emptyMigrator, document[pathMap.sourcePath], document);
                }

                debug('Migrate: %s -> %s', pathMap.sourcePath, pathMap.destinationPath);
                return Q.ninvoke(pathMap, 'migrator', document[pathMap.sourcePath], document);
              })
              .then(function migratorSuccessCallback(value) {
                debug('Successfully migrated %s -> %s. Saving document "%s".', pathMap.sourcePath, pathMap.destinationPath, document._id);

                document.set(pathMap.destinationPath, value);

                return Q.ninvoke(document, 'save');
              })
              .nodeify(next);
          })
          .nodeify(next);
        });
      })
      .nodeify(callback);
  };
};


