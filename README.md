# Nitix-db 

A lightweight, file-based JSON document store for Node.js. Provides a MongoDB-like API (collections, `insert`/`find`/`update`/`delete`) backed by a single JSON file on disk, with an in-memory mutex to serialize file access.

## Features

- Simple collection-based API (`createCollection`, `insert`, `find`, `update`, `delete`, `dropCollection`)
- Auto-generated `_id`, `_createdAt`, `_updatedAt` fields on every document
- Query by exact field match, with `sort`, `skip`, and `limit` support
- Chainable per-collection handle via `db.collection(name)`
- Descriptive errors via a single `DatabaseError` class with error codes
- `getStats()` for collection sizes/counts

## Installation

```bash
npm install nitix-db@github:Lord-Samuel/Nitix-db
```

## Quick Start

```javascript
const nix = require('nitix-db');

async function main() {
  // Initialize (creates the file if it doesn't exist)
  const db = await nix.create('database.json');

  await db.createCollection('users');

  const { insertedIds } = await db.insert('users', {
    name: 'Ada Lovelace',
    role: 'admin'
  });

  const { data } = await db.find('users', { role: 'admin' });
  console.log(data);

  await db.update('users', { name: 'Ada Lovelace' }, { role: 'superadmin' });

  await db.delete('users', { name: 'Ada Lovelace' });
}

main();
```

You can also use the standard constructor + `init()` instead of the static `create()` helper:

```javascript
const db = new nix('database.json');
await db.init();
```

## Collection Handle API

For repeated use on one collection, grab a scoped handle:

```javascript
const users = db.collection('users');

await users.insert({ name: 'Grace Hopper' });
await users.find({ name: 'Grace Hopper' });
await users.findOne({ name: 'Grace Hopper' });
await users.update({ name: 'Grace Hopper' }, { role: 'admin' });
await users.delete({ name: 'Grace Hopper' });
await users.count();
await users.drop();
```

## API Reference

### `JSONDatabase.create(filePath)` (static)
Creates and initializes a database in one call. Returns a ready-to-use instance.

### `new JSONDatabase(filePath)` / `db.init()`
Manual construction. `filePath` defaults to `'database.json'`. Call `init()` before any other method.

### `db.createCollection(name)`
Creates an empty collection. Throws `COLLECTION_EXISTS` if it already exists.

### `db.insert(collection, documents)`
Accepts a single document or an array. Adds `_id`, `_createdAt`, `_updatedAt` to each. Returns `{ success, insertedCount, insertedIds }`.

### `db.find(collection, query = {}, options = {})`
`query` is a plain object of exact-match field/value pairs (`{}` matches everything). `options.sort` is `{ field: 1 | -1 }`; `options.limit` / `options.skip` paginate. Returns `{ success, data, count }`.

### `db.findOne(collection, query = {})`
Same as `find` with `limit: 1`. Returns `{ success, data, count }` where `data` is a single document or `null`.

### `db.update(collection, query, update, options = { multi: false })`
Merges `update` into matched document(s) and bumps `_updatedAt`. With `multi: false` (default), only the first match is updated. Throws `DOCUMENT_NOT_FOUND` if nothing matches and `options.upsert` isn't set.

### `db.delete(collection, query, options = { multi: false })`
Removes matched document(s). Throws `DOCUMENT_NOT_FOUND` if nothing matches.

### `db.dropCollection(name)`
Deletes an entire collection.

### `db.count(collection, query = {})`
Returns `{ success, count }` for matching documents.

### `db.getStats()`
Returns per-collection document counts and byte sizes.

### `db.collection(name)`
Returns a scoped object with `insert`, `find`, `findOne`, `update`, `delete`, `drop`, and `count`, all pre-bound to `name`.

### `db.close()`
Marks the instance as uninitialized. Any further calls require re-`init()`.

## Error Handling

All errors are instances of `DatabaseError`:

```javascript
try {
  await db.insert('missingCollection', { foo: 'bar' });
} catch (err) {
  if (err instanceof JSONDatabase.DatabaseError) {
    console.error(err.code, err.message); // e.g. "COLLECTION_NOT_FOUND"
  }
}
```
