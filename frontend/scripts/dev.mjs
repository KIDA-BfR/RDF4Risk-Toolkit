import { spawn } from 'node:child_process';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const frontendDir = path.resolve(__dirname, '..');
const repoRoot = path.resolve(__dirname, '../..');
const backendPort = process.env.RDF4RISK_BACKEND_PORT || '8766';
const backendHost = process.env.RDF4RISK_BACKEND_HOST || '127.0.0.1';
const frontendHost = process.env.RDF4RISK_FRONTEND_HOST || '127.0.0.1';
const python = process.env.PYTHON || 'python';
const reuseBackend = process.env.RDF4RISK_REUSE_BACKEND === '1';

const children = [];
const backendBaseUrl = `http://${backendHost}:${backendPort}`;

function backendPortStatus() {
  return new Promise((resolve) => {
    const request = http.get(`${backendBaseUrl}/api/health`, { timeout: 1000 }, (response) => {
      let body = '';
      response.setEncoding('utf8');
      response.on('data', (chunk) => {
        body += chunk;
      });
      response.on('end', () => {
        try {
          const payload = JSON.parse(body);
          resolve(response.statusCode === 200 && payload.ok === true && Array.isArray(payload.services) ? 'healthy' : 'occupied');
        } catch {
          resolve('occupied');
        }
      });
    });
    request.on('error', () => resolve('available'));
    request.on('timeout', () => {
      request.destroy();
      resolve('occupied');
    });
  });
}

function start(name, command, args, options) {
  const child = spawn(command, args, { stdio: 'inherit', shell: false, ...options });
  children.push(child);
  child.on('exit', (code, signal) => {
    if (signal) return;
    if (code && code !== 0) {
      console.error(`${name} exited with code ${code}`);
      shutdown(code);
    }
  });
  return child;
}

function shutdown(code = 0) {
  for (const child of children) {
    if (!child.killed) child.kill('SIGTERM');
  }
  process.exit(code);
}

process.on('SIGINT', () => shutdown(0));
process.on('SIGTERM', () => shutdown(0));

const backendStatus = await backendPortStatus();
if (backendStatus === 'healthy') {
  if (!reuseBackend) {
    console.error(
      `A RDF4Risk MUI backend is already running at ${backendBaseUrl}. ` +
        'npm run start no longer reuses an existing backend by default, because that can keep old code, uploaded files, and UI settings alive in memory. ' +
        'Stop the old backend first, or set RDF4RISK_REUSE_BACKEND=1 if you intentionally want to reuse it.',
    );
    process.exit(1);
  }
  console.log(`python backend already running at ${backendBaseUrl}; reusing it because RDF4RISK_REUSE_BACKEND=1`);
} else if (backendStatus === 'occupied') {
  console.error(
    `Port ${backendPort} on ${backendHost} is already in use, but it is not an RDF4Risk MUI backend. ` +
      'Stop that process or set RDF4RISK_BACKEND_PORT to a free port.',
  );
  process.exit(1);
} else {
  start('python backend', python, ['mui_backend_server.py', '--host', backendHost, '--port', backendPort], { cwd: repoRoot });
}
start(
  'vite frontend',
  process.platform === 'win32' ? 'npm.cmd' : 'npm',
  ['run', 'start:frontend', '--', '--host', frontendHost],
  {
    cwd: frontendDir,
    env: {
      ...process.env,
      VITE_RDF4RISK_API_BASE: backendBaseUrl,
    },
  },
);
