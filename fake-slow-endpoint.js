import http from 'http';

const DELAY_MS = 15000; // 15 seconds - long enough to clearly observe, short enough to not wait forever

const server = http.createServer((req, res) => {
  console.log(`[slow-endpoint] Request received, holding for ${DELAY_MS / 1000}s before responding...`);

  setTimeout(() => {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ received: true }));
    console.log('[slow-endpoint] Finally responded.');
  }, DELAY_MS);
});

server.listen(9998, () => {
  console.log('Fake slow endpoint listening on http://localhost:9998');
});