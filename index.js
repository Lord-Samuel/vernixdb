const db = require('./src/index');

module.exports = db;
module.exports.inError = require('./src/index').DatabaseError;