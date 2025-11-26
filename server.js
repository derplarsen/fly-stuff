/**
 * ============================================================================
 * Motherduck Proxy Server
 * ============================================================================
 * 
 * A simple proxy server that:
 * - Reads from Motherduck (fast!)
 * - Writes to both Motherduck AND Google Sheets (backup)
 * - Provides REST API for your HTML apps
 * 
 * Deploy to: Vercel, Railway, Render, Fly.io, or any Node.js host
 * 
 * ============================================================================
 */
const port = process.env.PORT || 4000

const express = require('express');
const cors = require('cors');
const duckdb = require('duckdb');
const fetch = require('node-fetch');

const app = express();
app.use(cors());
app.use(express.json());

// ============================================================================
// CONFIGURATION
// ============================================================================

const CONFIG = {
    // Motherduck credentials
    motherduck: {
        // Prefer environment variable, fall back to hardcoded token
        token: process.env.MOTHERDUCK_TOKEN || process.env.motherduck_token || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InNhLTU4ZjMyN2IyLTVjODktNDQyYy05MTQ0LWQxMDI2MzFjZDg1OUBzYS5tb3RoZXJkdWNrLmNvbSIsIm1kUmVnaW9uIjoiYXdzLXVzLWVhc3QtMSIsInNlc3Npb24iOiJzYS01OGYzMjdiMi01Yzg5LTQ0MmMtOTE0NC1kMTAyNjMxY2Q4NTkuc2EubW90aGVyZHVjay5jb20iLCJwYXQiOiI0TmZTOEtzNmtlYzQ3SHBLWFoyRVd0UDZRNHZvLWtSODhqSGIxRGNNN2dNIiwidXNlcklkIjoiMTI0YzZmYWItNzA2NS00MGYzLWJkNmItZDJhNGM4MmQzMjBjIiwiaXNzIjoibWRfcGF0IiwicmVhZE9ubHkiOmZhbHNlLCJ0b2tlblR5cGUiOiJyZWFkX3dyaXRlIiwiaWF0IjoxNzY0MDgwNjk0fQ.gkYzQhZFgb5cox92J5qaMFovYi42hnyzCEx--0t-sHg',
        // Database name - should match what's in your Motherduck account
        database: process.env.MOTHERDUCK_DATABASE || 'PartnerPortal'
    },
    
    // Google Sheets backup (optional)
    googleSheets: {
        enabled: process.env.GOOGLE_SHEETS_BACKUP !== 'false',
        appsScriptUrl: process.env.APPS_SCRIPT_URL || 'https://script.google.com/macros/s/AKfycbxLzfUIIJUr3WwhMpC6JvWOVdGoppiKEToA9nHhZdFYmeiGz_TL8YbTqxspRLKIJnYiMA/exec'
    },
    
    // Server
    port: process.env.PORT || 3000
};

// Table name mappings
const TABLE_MAP = {
    'Companies': 'Companies',
    'Contacts': 'Contacts',
    'LOI Submissions': 'LOI_Submissions',
    'LOI_Submissions': 'LOI_Submissions',
    'Touchpoints': 'Touchpoints',
    'Blog Entries': 'Blog_Entries',
    'Blog_Entries': 'Blog_Entries',
    'Campaigns': 'Campaigns',
    'Contact Form Submissions': 'Contact_Form_Submissions',
    'Contact_Form_Submissions': 'Contact_Form_Submissions',
    'Templates': 'Templates'
};

// ============================================================================
// DATABASE CONNECTION
// ============================================================================

let db = null;
let conn = null;

async function initDatabase() {
    return new Promise((resolve, reject) => {
        console.log('🔄 Initializing Motherduck connection...');
        
        // Create DuckDB instance
        db = new duckdb.Database(':memory:');
        conn = db.connect();
        
        // Helper to run SQL commands sequentially
        const runSQL = (sql) => new Promise((res, rej) => {
            conn.run(sql, (err) => err ? rej(err) : res());
        });
        
        (async () => {
            try {
                // Install and load Motherduck extension
                console.log('📦 Installing Motherduck extension...');
                await runSQL("INSTALL motherduck");
                await runSQL("LOAD motherduck");
                console.log('✅ Motherduck extension loaded');
                
                // Set the token FIRST (as a secret)
                console.log('🔑 Setting authentication token...');
                await runSQL(`SET motherduck_token = '${CONFIG.motherduck.token}'`);
                
                // Now attach the database (without token in URL)
                console.log(`📎 Attaching database: ${CONFIG.motherduck.database}...`);
                await runSQL(`ATTACH 'md:${CONFIG.motherduck.database}'`);
                
                console.log('✅ Connected to Motherduck database:', CONFIG.motherduck.database);
                resolve();
                
            } catch (err) {
                console.error('❌ Database initialization failed:', err.message);
                console.log('');
                console.log('Troubleshooting:');
                console.log('  1. Make sure MOTHERDUCK_TOKEN env var is set correctly');
                console.log('  2. Verify database name exists in your Motherduck account');
                console.log('  3. Check that your token has access to this database');
                console.log('');
                console.log('To use environment token, set: export MOTHERDUCK_TOKEN=your_token');
                reject(err);
            }
        })();
    });
}

// ============================================================================
// DATABASE QUERY HELPERS
// ============================================================================

function runQuery(sql) {
    return new Promise((resolve, reject) => {
        conn.all(sql, (err, rows) => {
            if (err) reject(err);
            else resolve(rows || []);
        });
    });
}

function runStatement(sql) {
    return new Promise((resolve, reject) => {
        conn.run(sql, (err) => {
            if (err) reject(err);
            else resolve({ success: true });
        });
    });
}

function escapeValue(value) {
    if (value === null || value === undefined) return 'NULL';
    if (typeof value === 'number') return value;
    if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
    if (Array.isArray(value)) return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
    return `'${String(value).replace(/'/g, "''")}'`;
}

function getTableName(name) {
    return TABLE_MAP[name] || name;
}

// ============================================================================
// GOOGLE SHEETS BACKUP
// ============================================================================

async function backupToGoogleSheets(action, data) {
    if (!CONFIG.googleSheets.enabled) return;
    
    try {
        const params = new URLSearchParams({
            action: action,
            data: JSON.stringify(data)
        });
        
        const response = await fetch(`${CONFIG.googleSheets.appsScriptUrl}?${params.toString()}`);
        const result = await response.json();
        
        if (result.success) {
            console.log('📋 Google Sheets backup successful');
        } else {
            console.warn('⚠️ Google Sheets backup warning:', result.message);
        }
    } catch (error) {
        console.warn('⚠️ Google Sheets backup failed:', error.message);
    }
}

// ============================================================================
// API ROUTES
// ============================================================================

// Health check
app.get('/', (req, res) => {
    res.json({ 
        status: 'ok', 
        service: 'Motherduck Proxy',
        database: CONFIG.motherduck.database
    });
});

// GET all records from a table
app.get('/api/:table', async (req, res) => {
    try {
        const tableName = getTableName(req.params.table);
        console.log(`📡 GET ${tableName}`);
        
        const sql = `SELECT * FROM ${CONFIG.motherduck.database}.${tableName}`;
        const rows = await runQuery(sql);
        
        console.log(`✅ Returned ${rows.length} rows`);
        res.json({ success: true, data: rows });
        
    } catch (error) {
        console.error('❌ Query error:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// GET single record by ID
app.get('/api/:table/:id', async (req, res) => {
    try {
        const tableName = getTableName(req.params.table);
        const id = req.params.id;
        
        const sql = `SELECT * FROM ${CONFIG.motherduck.database}.${tableName} WHERE id = ${id}`;
        const rows = await runQuery(sql);
        
        if (rows.length === 0) {
            res.status(404).json({ success: false, error: 'Not found' });
        } else {
            res.json({ success: true, data: rows[0] });
        }
        
    } catch (error) {
        console.error('❌ Query error:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// POST - Insert new record
app.post('/api/:table', async (req, res) => {
    try {
        const tableName = getTableName(req.params.table);
        const data = req.body;
        
        console.log(`💾 INSERT into ${tableName}`);
        
        // Generate ID if not provided
        if (!data.id) {
            const maxIdResult = await runQuery(
                `SELECT COALESCE(MAX(id), 0) as maxId FROM ${CONFIG.motherduck.database}.${tableName}`
            );
            data.id = (maxIdResult[0]?.maxId || 0) + 1;
        }
        
        const columns = Object.keys(data);
        const values = columns.map(col => escapeValue(data[col]));
        
        const sql = `INSERT INTO ${CONFIG.motherduck.database}.${tableName} 
                     (${columns.join(', ')}) VALUES (${values.join(', ')})`;
        
        await runStatement(sql);
        console.log('✅ Insert successful');
        
        // Backup to Google Sheets
        const singularName = tableName.endsWith('s') ? tableName.slice(0, -1) : tableName;
        await backupToGoogleSheets(`save${singularName}`, { [singularName.toLowerCase()]: data });
        
        res.json({ success: true, data: data });
        
    } catch (error) {
        console.error('❌ Insert error:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// PUT - Update existing record
app.put('/api/:table/:id', async (req, res) => {
    try {
        const tableName = getTableName(req.params.table);
        const id = req.params.id;
        const data = req.body;
        data.id = parseInt(id);
        
        console.log(`📝 UPDATE ${tableName} id=${id}`);
        
        const updates = Object.keys(data)
            .filter(key => key !== 'id')
            .map(key => `${key} = ${escapeValue(data[key])}`)
            .join(', ');
        
        const sql = `UPDATE ${CONFIG.motherduck.database}.${tableName} 
                     SET ${updates} WHERE id = ${id}`;
        
        await runStatement(sql);
        console.log('✅ Update successful');
        
        // Backup to Google Sheets
        const singularName = tableName.endsWith('s') ? tableName.slice(0, -1) : tableName;
        await backupToGoogleSheets(`update${singularName}`, { [singularName.toLowerCase()]: data });
        
        res.json({ success: true, data: data });
        
    } catch (error) {
        console.error('❌ Update error:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// DELETE - Remove record
app.delete('/api/:table/:id', async (req, res) => {
    try {
        const tableName = getTableName(req.params.table);
        const id = req.params.id;
        
        console.log(`🗑️ DELETE from ${tableName} id=${id}`);
        
        const sql = `DELETE FROM ${CONFIG.motherduck.database}.${tableName} WHERE id = ${id}`;
        
        await runStatement(sql);
        console.log('✅ Delete successful');
        
        // Backup to Google Sheets
        const singularName = tableName.endsWith('s') ? tableName.slice(0, -1) : tableName;
        await backupToGoogleSheets(`delete${singularName}`, { id: parseInt(id) });
        
        res.json({ success: true });
        
    } catch (error) {
        console.error('❌ Delete error:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Custom SQL query (GET for simple queries)
app.get('/api/query', async (req, res) => {
    try {
        const sql = req.query.sql;
        if (!sql) {
            return res.status(400).json({ success: false, error: 'Missing sql parameter' });
        }
        
        console.log(`🔍 Custom query: ${sql.substring(0, 100)}...`);
        const rows = await runQuery(sql);
        
        res.json({ success: true, data: rows });
        
    } catch (error) {
        console.error('❌ Query error:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ============================================================================
// START SERVER
// ============================================================================

async function start() {
    try {
        await initDatabase();
        
        app.listen(port, () => {
            console.log('');
            console.log('🚀 Motherduck Proxy Server running!');
            console.log(`   URL: http://localhost:${CONFIG.port}`);
            console.log(`   Database: ${CONFIG.motherduck.database}`);
            console.log(`   Google Sheets backup: ${CONFIG.googleSheets.enabled ? 'enabled' : 'disabled'}`);
            console.log('');
            console.log('API Endpoints:');
            console.log(`   GET    /api/{table}      - Get all records`);
            console.log(`   GET    /api/{table}/{id} - Get single record`);
            console.log(`   POST   /api/{table}      - Insert record`);
            console.log(`   PUT    /api/{table}/{id} - Update record`);
            console.log(`   DELETE /api/{table}/{id} - Delete record`);
            console.log('');
            console.log('Tables: Companies, Contacts, Touchpoints, Campaigns,');
            console.log('        Templates, Blog_Entries, LOI_Submissions');
            console.log('');
        });
        
    } catch (error) {
        console.error('❌ Failed to start server:', error.message);
        process.exit(1);
    }
}

start();
