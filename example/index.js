const nix = require('./vernixdb');

async function example() {
    try {
        // Initialize database
        const db = new nix('my-database.json');
        await db.init();

        // Create a collection
        await db.createCollection('users');

        // Insert documents
        await db.insert('users', [
            { name: 'Kofi', age: 30, email: 'him@example.com' },
            { name: 'Jane', age: 25, email: 'jane@example.com' }
        ]);

        // Find documents
        const result = await db.find('users', { age: { $gte: 25 } });
        console.log('Found users:', result.data);

        // Update documents
        await db.update('users', { name: 'kofi' }, { age: 31 });

        // Get stats
        const stats = await db.getStats();
        console.log('Database stats:', stats);

    } catch (error) {
        if (error instanceof nix.inError) {
            console.error('Database error:', error.toJSON());
        } else {
            console.error('Unexpected error:', error);
        }
    }
}
example();

