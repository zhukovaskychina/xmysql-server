const mysql = (() => { try { return require('mysql2/promise'); } catch (_) { return null; } })();
if (!mysql) { console.error('mysql2 is not installed'); process.exit(125); }
const fs = require('fs');
const crypto = require('crypto');

(async () => {
  const options = {host: process.env.XMYSQL_CLIENT_HOST, port: Number(process.env.XMYSQL_CLIENT_PORT), user: process.env.XMYSQL_CLIENT_USER, password: process.env.XMYSQL_CLIENT_PASSWORD, database: 'mysql', charset: 'utf8mb4', multipleStatements: true, connectTimeout: 10000};
  if (process.env.XMYSQL_CLIENT_TLS_CA) {
    options.ssl = {ca: fs.readFileSync(process.env.XMYSQL_CLIENT_TLS_CA), rejectUnauthorized: true};
  }
  let connection = await mysql.createConnection(options);
  const cases = {};
  const passed = (name) => { cases[name] = 'PASS'; };
  const runAuthPluginCase = async () => {
    const password = process.env.XMYSQL_CLIENT_AUTH_PLUGIN_PASSWORD || crypto.randomBytes(24).toString('hex');
    const accounts = [
      {user: 'xmysql_cache_client', plugin: 'caching_sha2_password'},
      {user: 'xmysql_sha_client', plugin: 'sha256_password'}
    ];
    for (const account of accounts) {
      await connection.query(`DROP USER IF EXISTS '${account.user}'@'%'`);
      await connection.query(`CREATE USER '${account.user}'@'%' IDENTIFIED WITH ${account.plugin} BY '${password}'`);
      await connection.query(`GRANT SELECT ON *.* TO '${account.user}'@'%'`);
    }
    try {
      for (const account of accounts) {
        const pluginOptions = {...options};
        delete pluginOptions.database;
        const pluginConnection = await mysql.createConnection({...pluginOptions, user: account.user, password});
        try {
          const [rows] = await pluginConnection.query('SELECT 1');
          if (rows[0]['1'] !== 1) throw new Error(`auth plugin query failed for ${account.user}`);
        } finally {
          await pluginConnection.end();
        }
      }
      passed('auth-plugins');
    } finally {
      for (const account of accounts) {
        await connection.query(`DROP USER IF EXISTS '${account.user}'@'%'`);
      }
    }
  };
  try {
    if (process.env.XMYSQL_CLIENT_AUTH_PLUGINS === '1') await runAuthPluginCase();
    const [one] = await connection.query('SELECT 1');
    if (one[0]['1'] !== 1) throw new Error('SELECT 1 failed');
    passed('connection-auth');
    await connection.query('CREATE DATABASE IF NOT EXISTS client_matrix');
    await connection.query('CREATE TABLE IF NOT EXISTS client_matrix.matrix_rows(id INT PRIMARY KEY, label VARCHAR(32))');
    await connection.execute('INSERT INTO client_matrix.matrix_rows(id, label) VALUES (?, ?) ON DUPLICATE KEY UPDATE label=VALUES(label)', [1, 'one']);
    passed('database-ddl-dml');
    const [prepared] = await connection.execute('SELECT ? + 1 AS value', [41]);
    if (prepared[0].value !== 42) throw new Error('prepared statement failed');
    passed('prepared-statements');
    await connection.beginTransaction();
    await connection.execute('INSERT INTO client_matrix.matrix_rows(id, label) VALUES (?, ?)', [2, 'tx']);
    await connection.rollback();
    passed('transactions');
    const [typed] = await connection.query("SELECT NULL AS nullable_value, CAST(42 AS SIGNED) AS integer_value, _utf8mb4'兼容' AS utf8mb4_value");
    if (typed[0].nullable_value !== null || typed[0].integer_value !== 42 || typed[0].utf8mb4_value !== '兼容') throw new Error('NULL/type/charset failed');
    passed('null-and-types');
    const [charset] = await connection.query("SELECT _utf8mb4'兼容' AS utf8mb4_value");
    if (charset[0].utf8mb4_value !== '兼容') throw new Error('charset failed');
    passed('charset');
    const [metadata] = await connection.query("SELECT COUNT(*) AS count FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix'");
    if (metadata[0].count <= 0) throw new Error('metadata failed');
    passed('metadata');
    const [metadataRows, metadataFields] = await connection.query("SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, COLUMN_NAME AS column_name, ORDINAL_POSITION AS ordinal_position FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix' ORDER BY TABLE_NAME, ORDINAL_POSITION");
    const metadataNames = metadataFields.map((field) => field.name.toLowerCase());
    const metadataRow = metadataRows[0] || {};
    const metadataValue = (name) => metadataRow[name] ?? metadataRow[name.toUpperCase()];
    if (metadataNames.join(',') !== 'table_schema,table_name,column_name,ordinal_position' || metadataRows.length === 0 || metadataValue('table_schema') !== 'client_matrix' || !metadataValue('table_name') || !metadataValue('column_name') || metadataValue('ordinal_position') < 1) throw new Error('metadata shape failed');
    passed('metadata-shape');
    await connection.query('CREATE TABLE IF NOT EXISTS client_matrix.auto_rows(id INT PRIMARY KEY AUTO_INCREMENT, label VARCHAR(32))');
    const [insertResult] = await connection.execute('INSERT INTO client_matrix.auto_rows(label) VALUES (?)', ['node-client']);
    if (insertResult.insertId <= 0 || insertResult.affectedRows !== 1) throw new Error('insert result metadata failed');
    passed('auto-increment-and-result-metadata');
    await connection.beginTransaction();
    await connection.execute("INSERT INTO client_matrix.matrix_rows(id, label) VALUES (300001, 'savepoint-before')");
    await connection.query('SAVEPOINT client_matrix_sp');
    await connection.execute("INSERT INTO client_matrix.matrix_rows(id, label) VALUES (300002, 'savepoint-after')");
    await connection.query('ROLLBACK TO SAVEPOINT client_matrix_sp');
    await connection.query('RELEASE SAVEPOINT client_matrix_sp');
    await connection.commit();
    const [savepointRows] = await connection.query('SELECT COUNT(*) AS count FROM client_matrix.matrix_rows WHERE id IN (300001, 300002)');
    if (savepointRows[0].count !== 1) throw new Error('savepoint rollback failed');
    await connection.query('DELETE FROM client_matrix.matrix_rows WHERE id IN (300001, 300002)');
    passed('savepoints');
    await connection.query('SET @client_matrix_value = 41');
    const [sessionRows] = await connection.query('SELECT @client_matrix_value + 1 AS value');
    if (sessionRows[0].value !== 42) throw new Error('session state failed');
    passed('session-state');
    await connection.ping();
    passed('protocol-ping');
    await connection.changeUser({user: process.env.XMYSQL_CLIENT_USER, password: process.env.XMYSQL_CLIENT_PASSWORD, database: 'client_matrix', charset: 'utf8mb4'});
    const [changedUserRows] = await connection.query('SELECT DATABASE() AS database_name');
    if (changedUserRows[0].database_name !== 'client_matrix') throw new Error('COM_CHANGE_USER database switch failed');
    passed('protocol-change-user');
    await connection.query('SET @client_matrix_reset = 41');
    await connection.reset();
    const [resetRows] = await connection.query('SELECT @client_matrix_reset AS value');
    if (resetRows[0].value !== null) throw new Error('session reset did not clear user variables');
    passed('session-reset');
    const [multi] = await connection.query('SELECT 1 AS first_col; SELECT 2 AS second_col');
    if (!Array.isArray(multi) || multi.length !== 2 || multi[0][0].first_col !== 1 || multi[1][0].second_col !== 2) throw new Error('multi-result failed');
    passed('multi-result-and-error');
    try {
      await connection.query('SELECT * FROM table_that_does_not_exist');
      throw new Error('missing table query unexpectedly succeeded');
    } catch (error) {
      if (!String(error.message || error).toLowerCase().includes('table')) throw error;
    }
    try {
      await connection.query('SELECT * FROM client_matrix_missing_table');
      throw new Error('missing table query unexpectedly succeeded');
    } catch (error) {
      if (Number(error.errno) !== 1146 || String(error.code) !== 'ER_NO_SUCH_TABLE') throw error;
    }
    passed('negative-error-code');
    await connection.query('CREATE TABLE IF NOT EXISTS client_matrix.type_rows(id BIGINT PRIMARY KEY, decimal_value DECIMAL(10,2), double_value DOUBLE, text_value TEXT, blob_value BLOB, created_at TIMESTAMP)');
    const [typeRows] = await connection.query("SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix' AND TABLE_NAME='type_rows' ORDER BY ORDINAL_POSITION");
    if (typeRows.map((row) => String(row.DATA_TYPE).toLowerCase()).join(',') !== 'bigint,decimal,double,text,blob,timestamp') throw new Error('extended type metadata failed');
    passed('extended-types-metadata');
    await connection.query('CREATE TABLE IF NOT EXISTS client_matrix.wire_rows(id INT PRIMARY KEY, decimal_value DECIMAL(10,2), double_value DOUBLE, binary_value BLOB, date_value DATE)');
    await connection.query('DELETE FROM client_matrix.wire_rows WHERE id = 1');
    await connection.query("INSERT INTO client_matrix.wire_rows(id, decimal_value, double_value, binary_value, date_value) VALUES (1, 12.34, 1.5, _binary'xy', '2026-09-28')");
    const [wireValues] = await connection.query('SELECT decimal_value, double_value, binary_value, date_value FROM client_matrix.wire_rows WHERE id = 1');
    const wireRow = wireValues[0];
    const binaryText = Buffer.isBuffer(wireRow.binary_value) ? wireRow.binary_value.toString() : String(wireRow.binary_value);
    const dateText = wireRow.date_value instanceof Date ? wireRow.date_value.toISOString().slice(0, 10) : String(wireRow.date_value);
    if (String(wireRow.decimal_value) !== '12.34' || Number(wireRow.double_value) !== 1.5 || binaryText !== 'xy' || dateText !== '2026-09-28') throw new Error('wire value types failed');
    passed('wire-value-types');
    const pool = mysql.createPool({host: process.env.XMYSQL_CLIENT_HOST, port: Number(process.env.XMYSQL_CLIENT_PORT), user: process.env.XMYSQL_CLIENT_USER, password: process.env.XMYSQL_CLIENT_PASSWORD, database: 'mysql', charset: 'utf8mb4', connectionLimit: 2, waitForConnections: true, queueLimit: 0});
    const [poolFirst, poolSecond] = await Promise.all([
      pool.query('SELECT 1 AS value'),
      pool.query('SELECT 2 AS value')
    ]);
    if (poolFirst[0][0].value !== 1 || poolSecond[0][0].value !== 2) throw new Error('multi-session pool queries failed');
    await pool.end();
    passed('multi-session-pool');
    await connection.end();
    connection = await mysql.createConnection({host: process.env.XMYSQL_CLIENT_HOST, port: Number(process.env.XMYSQL_CLIENT_PORT), user: process.env.XMYSQL_CLIENT_USER, password: process.env.XMYSQL_CLIENT_PASSWORD, database: 'mysql', charset: 'utf8mb4', multipleStatements: true});
    const [reconnected] = await connection.query('SELECT 1');
    if (reconnected[0]['1'] !== 1) throw new Error('reconnect failed');
    passed('reconnect');
    await connection.end();
    console.log(JSON.stringify({client: 'node-mysql2', cases}));
  } catch (error) { console.error(error.stack || error); process.exit(1); }
  finally { if (connection) await connection.end().catch(() => {}); }
})();
