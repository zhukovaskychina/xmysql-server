const mysql = (() => { try { return require('mysql2/promise'); } catch (_) { return null; } })();
if (!mysql) { console.error('mysql2 is not installed'); process.exit(125); }

const fs = require('fs');

(async () => {
  const config = {host: process.env.XMYSQL_RECONNECT_HOST, port: Number(process.env.XMYSQL_RECONNECT_PORT), user: process.env.XMYSQL_CLIENT_USER, password: process.env.XMYSQL_CLIENT_PASSWORD, database: 'mysql'};
  const readyPath = process.env.XMYSQL_RECONNECT_READY_FILE;
  const serverReadyPath = process.env.XMYSQL_RECONNECT_SERVER_READY_FILE;
  const timeoutMs = Number(process.env.XMYSQL_RECONNECT_TIMEOUT_SECONDS || 30) * 1000;
  let connection = await mysql.createConnection(config);
  await connection.query('SELECT 1');
  fs.writeFileSync(readyPath, 'ready\n');
  const deadline = Date.now() + timeoutMs;
  while (!fs.existsSync(serverReadyPath)) {
    if (Date.now() >= deadline) throw new Error('server ready marker did not appear');
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  try {
    await connection.query('SELECT 1');
    throw new Error('old physical connection unexpectedly succeeded after server restart');
  } catch (error) {
    if (String(error.message || error).includes('unexpectedly succeeded')) throw error;
  }
  await connection.destroy();
  connection = await mysql.createConnection(config);
  await connection.query('SELECT 1');
  await connection.end();
  const recoveryCase = process.env.XMYSQL_RECONNECT_FAULT_MODE === 'network' ? 'reconnect-after-network-fault' : 'reconnect-after-server-restart';
  console.log(JSON.stringify({client: 'node-mysql2', cases: {'old-connection-failure': 'PASS', [recoveryCase]: 'PASS'}}));
})().catch((error) => { console.error(error.stack || error); process.exit(1); });
