'use strict';
const { startNodeServer } = require('./node-server');

const args = Object.fromEntries(process.argv.slice(2).map((arg) => {
  const [key, value] = arg.replace(/^--/, '').split('=');
  return [key, value];
}));

const bootstrap = args['bootstrap-id'] ? {
  id: Number(args['bootstrap-id']),
  host: args['bootstrap-host'],
  port: Number(args['bootstrap-port'])
} : null;

startNodeServer({
  id: Number(args.id),
  host: args.host,        // IP real desta máquina
  port: Number(args.port || 5000)
}).then(async ({ node }) => {
  await node.join(bootstrap);
  console.log(`Nó ${node.id} em http://${node.host}:${node.port}`);
});