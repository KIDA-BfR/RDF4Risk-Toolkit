import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const frontendDir = path.resolve(__dirname, '..');
const repoRoot = path.resolve(__dirname, '../..');
const backendPort = process.env.RDF4RISK_BACKEND_PORT || '8765';
const backendHost = process.env.RDF4RISK_BACKEND_HOST || '127.0.0.1';
const python = process.env.PYTHON || 'python';

const children = [];

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

start('python backend', python, ['mui_backend_server.py', '--host', backendHost, '--port', backendPort], { cwd: repoRoot });
start(
  'vite frontend',
  process.platform === 'win32' ? 'npm.cmd' : 'npm',
  ['run', 'start:frontend', '--', '--host', '0.0.0.0'],
  {
    cwd: frontendDir,
    env: {
      ...process.env,
      VITE_RDF4RISK_API_BASE: `http://${backendHost}:${backendPort}`,
    },
  },
);