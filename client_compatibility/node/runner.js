const mysql = (() => { try { return require('mysql2/promise'); } catch (_) { return null; } })();
if (!mysql) { console.error('mysql2 is not installed'); process.exit(125); }

(async () => {
  let connection = await mysql.createConnection({host: process.env.XMYSQL_CLIENT_HOST, port: Number(process.env.XMYSQL_CLIENT_PORT), user: process.env.XMYSQL_CLIENT_USER, password: process.env.XMYSQL_CLIENT_PASSWORD, database: 'mysql', charset: 'utf8mb4', multipleStatements: true});
  const cases = {};
  const passed = (name) => { cases[name] = 'PASS'; };
  try {
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
    const [multi] = await connection.query('SELECT 1 AS first_col; SELECT 2 AS second_col');
    if (!Array.isArray(multi) || multi.length !== 2 || multi[0][0].first_col !== 1 || multi[1][0].second_col !== 2) throw new Error('multi-result failed');
    passed('multi-result-and-error');
    try {
      await connection.query('SELECT * FROM table_that_does_not_exist');
      throw new Error('missing table query unexpectedly succeeded');
    } catch (error) {
      if (!String(error.message || error).toLowerCase().includes('table')) throw error;
    }
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
