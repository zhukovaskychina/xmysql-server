const mysql = (() => { try { return require('mysql2/promise'); } catch (_) { return null; } })();
if (!mysql) { console.error('mysql2 is not installed'); process.exit(125); }

(async () => {
  const connection = await mysql.createConnection({host: process.env.XMYSQL_CLIENT_HOST, port: Number(process.env.XMYSQL_CLIENT_PORT), user: process.env.XMYSQL_CLIENT_USER, password: process.env.XMYSQL_CLIENT_PASSWORD, charset: 'utf8mb4'});
  try {
    const [one] = await connection.query('SELECT 1');
    if (one[0]['1'] !== 1) throw new Error('SELECT 1 failed');
    await connection.query('CREATE DATABASE IF NOT EXISTS client_matrix');
    await connection.query('CREATE TABLE IF NOT EXISTS client_matrix.matrix_rows(id INT PRIMARY KEY, label VARCHAR(32))');
    await connection.execute('INSERT INTO client_matrix.matrix_rows(id, label) VALUES (?, ?) ON DUPLICATE KEY UPDATE label=VALUES(label)', [1, 'one']);
    const [prepared] = await connection.execute('SELECT ? + 1 AS value', [41]);
    if (prepared[0].value !== 42) throw new Error('prepared statement failed');
    await connection.beginTransaction();
    await connection.execute('INSERT INTO client_matrix.matrix_rows(id, label) VALUES (?, ?)', [2, 'tx']);
    await connection.rollback();
    const [typed] = await connection.query("SELECT NULL AS nullable_value, CAST(42 AS SIGNED) AS integer_value, _utf8mb4'兼容' AS utf8mb4_value");
    if (typed[0].nullable_value !== null || typed[0].integer_value !== 42 || typed[0].utf8mb4_value !== '兼容') throw new Error('NULL/type/charset failed');
    const [metadata] = await connection.query("SELECT COUNT(*) AS count FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='client_matrix'");
    if (metadata[0].count <= 0) throw new Error('metadata failed');
    await connection.end();
    console.log(JSON.stringify({client: 'node-mysql2', status: 'PASS'}));
  } catch (error) { console.error(error.stack || error); process.exit(1); }
})();
