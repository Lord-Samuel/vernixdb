const fs = require('fs').promises;
const path = require('path');
const { Mutex } = require('async-mutex');

class JSONDatabase {
    constructor(filePath = 'database.json') {
        this.filePath = path.resolve(filePath);
        this.mutex = new Mutex();
        this.initialized = false;
    }

    async init() {
        const release = await this.mutex.acquire();
        
        try {
            try {
                await fs.access(this.filePath);
                const data = await fs.readFile(this.filePath, 'utf8');
                JSON.parse(data); // Validate Json
            } catch (error) {
                if (error.code === 'ENOENT') {
                    // If file doesn't exist, create new collection, seems logical
                    await fs.writeFile(this.filePath, JSON.stringify({}, null, 2));
                } else if (error instanceof SyntaxError) {
                    throw new DatabaseError('CORRUPTED_DATABASE', 'Database file contains invalid JSON');
                } else {
                    throw error
                }
            }
            
            this.initialized = true;
        } finally {
            release();
        }
    }

    async _readDatabase() {
        if (!this.initialized) {
            throw new DatabaseError('NOT_INITIALIZED', 'Database must be initialized first');
        }

        const release = await this.mutex.acquire();
        
        try {
            const data = await fs.readFile(this.filePath, 'utf8');
            return JSON.parse(data);
        } catch (error) {
            if (error instanceof SyntaxError) {
                throw new DatabaseError('CORRUPTED_DATABASE', 'Failed to parse database file');
            }
            throw new DatabaseError('READ_ERROR', `Failed to read database: ${error.message}`);
        } finally {
            release();
        }
    }

    async _writeDatabase(data) {
        if (!this.initialized) {
            throw new DatabaseError('NOT_INITIALIZED', 'Database must be initialized first');
        }

        const release = await this.mutex.acquire();
        
        try {
            await fs.writeFile(this.filePath, JSON.stringify(data, null, 2));
        } catch (error) {
            throw new DatabaseError('WRITE_ERROR', `Failed to write to database: ${error.message}`);
        } finally {
            release();
        }
    }

    async createCollection(collectionName) {
        try {
            if (typeof collectionName !== 'string' || !collectionName.trim()) {
                throw new DatabaseError('INVALID_INPUT', 'Collection name must be a non-empty string');
            }

            const db = await this._readDatabase();
            
            if (db[collectionName]) {
                throw new DatabaseError('COLLECTION_EXISTS', `Collection '${collectionName}' already exists`);
            }

            db[collectionName] = [];
            await this._writeDatabase(db);
            
            return { success: true, message: `Collection '${collectionName}' created` };
        } catch (error) {
            if (error instanceof DatabaseError) throw error;
            throw new DatabaseError('CREATE_COLLECTION_ERROR', `Failed to create collection: ${error.message}`);
        }
    }

    async insert(collectionName, documents) {
        try {
            if (!Array.isArray(documents)) {
                documents = [documents];
            }

            if (documents.length === 0) {
                throw new DatabaseError('INVALID_INPUT', 'No documents provided for insertion');
            }

            const db = await this._readDatabase();
            
            if (!db[collectionName]) {
                throw new DatabaseError('COLLECTION_NOT_FOUND', `Collection '${collectionName}' does not exist`);
            }

            const timestamp = new Date().toISOString();
            const documentsWithMetadata = documents.map(doc => ({
                ...doc,
                _id: this._generateId(),
                _createdAt: timestamp,
                _updatedAt: timestamp
            }));

            db[collectionName].push(...documentsWithMetadata);
            await this._writeDatabase(db);

            return {
                success: true,
                insertedCount: documentsWithMetadata.length,
                insertedIds: documentsWithMetadata.map(doc => doc._id)
            };
        } catch (error) {
            if (error instanceof DatabaseError) throw error;
            throw new DatabaseError('INSERT_ERROR', `Failed to insert documents: ${error.message}`);
        }
    }

    async find(collectionName, query = {}, options = {}) {
        try {
            const db = await this._readDatabase();
            
            if (!db[collectionName]) {
                throw new DatabaseError('COLLECTION_NOT_FOUND', `Collection '${collectionName}' does not exist`);
            }

            let results = db[collectionName].filter(doc => this._matchesQuery(doc, query));

            if (options.sort) {
                results = this._sortDocuments(results, options.sort);
            }

            if (options.limit) {
                const skip = options.skip || 0;
                results = results.slice(skip, skip + options.limit);
            }

            return {
                success: true,
                data: results,
                count: results.length
            };
        } catch (error) {
            if (error instanceof DatabaseError) throw error;
            throw new DatabaseError('FIND_ERROR', `Failed to find documents: ${error.message}`);
        }
    }

    async findOne(collectionName, query = {}) {
        try {
            const result = await this.find(collectionName, query, { limit: 1 });
            return {
                success: true,
                data: result.data[0] || null,
                count: result.data.length ? 1 : 0
            };
        } catch (error) {
            if (error instanceof DatabaseError) throw error;
            throw new DatabaseError('FIND_ERROR', `Failed to find document: ${error.message}`);
        }
    }

    async update(collectionName, query, update, options = { multi: false }) {
        try {
            const db = await this._readDatabase();
            
            if (!db[collectionName]) {
                throw new DatabaseError('COLLECTION_NOT_FOUND', `Collection '${collectionName}' does not exist`);
            }

            let updateCount = 0;
            const timestamp = new Date().toISOString();

            db[collectionName] = db[collectionName].map(doc => {
                if (this._matchesQuery(doc, query)) {
                    if (options.multi || updateCount === 0) {
                        updateCount++;
                        const updatedDoc = {
                            ...doc,
                            ...update,
                            _updatedAt: timestamp,
                            _id: doc._id 
                        };
                        return updatedDoc;
                    }
                }
                return doc;
            });

            if (updateCount === 0 && !options.upsert) {
                throw new DatabaseError('DOCUMENT_NOT_FOUND', 'No documents matched the query');
            }

            await this._writeDatabase(db);

            return {
                success: true,
                matchedCount: updateCount,
                modifiedCount: updateCount
            };
        } catch (error) {
            if (error instanceof DatabaseError) throw error;
            throw new DatabaseError('UPDATE_ERROR', `Failed to update documents: ${error.message}`);
        }
    }

    async delete(collectionName, query, options = { multi: false }) {
        try {
            const db = await this._readDatabase();
            
            if (!db[collectionName]) {
                throw new DatabaseError('COLLECTION_NOT_FOUND', `Collection '${collectionName}' does not exist`);
            }

            const originalLength = db[collectionName].length;
            db[collectionName] = db[collectionName].filter(doc => !this._matchesQuery(doc, query));
            const deletedCount = originalLength - db[collectionName].length;

            if (deletedCount === 0) {
                throw new DatabaseError('DOCUMENT_NOT_FOUND', 'No documents matched the query');
            }

            await this._writeDatabase(db);

            return {
                success: true,
                deletedCount
            };
        } catch (error) {
            if (error instanceof DatabaseError) throw error;
            throw new DatabaseError('DELETE_ERROR', `Failed to delete documents: ${error.message}`);
        }
    }

    async dropCollection(collectionName) {
        try {
            const db = await this._readDatabase();
            
            if (!db[collectionName]) {
                throw new DatabaseError('COLLECTION_NOT_FOUND', `Collection '${collectionName}' does not exist`);
            }

            delete db[collectionName];
            await this._writeDatabase(db);

            return { success: true, message: `Collection '${collectionName}' dropped` };
        } catch (error) {
            if (error instanceof DatabaseError) throw error;
            throw new DatabaseError('DROP_ERROR', `Failed to drop collection: ${error.message}`);
        }
    }

    collection(collectionName) {
        if (!this.initialized) {
            throw new DatabaseError('NOT_INITIALIZED', 'Database must be initialized first');
        }

        return {
            insert: (documents) => this.insert(collectionName, documents),
            find: (query, options) => this.find(collectionName, query, options),
            findOne: (query) => this.findOne(collectionName, query),
            update: (query, update, options) => this.update(collectionName, query, update, options),
            delete: (query, options) => this.delete(collectionName, query, options),
            drop: () => this.dropCollection(collectionName),
            count: async (query = {}) => {
                const result = await this.find(collectionName, query);
                return {
                    success: true,
                    count: result.count
                };
            }
        };
    }

    async count(collectionName, query = {}) {
        const result = await this.find(collectionName, query);
        return {
            success: true,
            count: result.count
        };
    }

    async close() {
        this.initialized = false;
        return { success: true, message: 'Database closed' };
    }

    static async create(filePath) {
        const db = new JSONDatabase(filePath);
        await db.init();
        return db;
    }

    _generateId() {
        return Date.now().toString(36) + Math.random().toString(36).substr(2);
    }

    _matchesQuery(doc, query) {
        if (Object.keys(query).length === 0) return true;

        for (const [key, value] of Object.entries(query)) {
            if (doc[key] !== value) {
                return false;
            }
        }
        return true;
    }

    _sortDocuments(documents, sort) {
        return documents.sort((a, b) => {
            for (const [key, direction] of Object.entries(sort)) {
                if (a[key] < b[key]) return direction === 1 ? -1 : 1;
                if (a[key] > b[key]) return direction === 1 ? 1 : -1;
            }
            return 0;
        });
    }

    async getStats() {
        try {
            const db = await this._readDatabase();
            const stats = {
                collections: {},
                totalCollections: Object.keys(db).length
            };

            for (const [collectionName, documents] of Object.entries(db)) {
                stats.collections[collectionName] = {
                    documentCount: documents.length,
                    size: Buffer.byteLength(JSON.stringify(documents), 'utf8')
                };
            }

            return { success: true, stats };
        } catch (error) {
            if (error instanceof DatabaseError) throw error;
            throw new DatabaseError('STATS_ERROR', `Failed to get database stats: ${error.message}`);
        }
    }
}

class DatabaseError extends Error {
    constructor(code, message) {
        super(message);
        this.name = 'DatabaseError';
        this.code = code;
        this.timestamp = new Date().toISOString();
    }

    toJSON() {
        return {
            error: true,
            code: this.code,
            message: this.message,
            timestamp: this.timestamp
        };
    }
}

// This is by far the most impressive thing I have done, btw this is based on my Whatsapp bot's Db

module.exports = JSONDatabase;
module.exports.DatabaseError = DatabaseError;