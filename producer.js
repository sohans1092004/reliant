// Test producer: sends one fake event through the ingest API.
//   node producer.js                    -> tenant_a
//   TEST_TENANT_ID=tenant_b node producer.js
import 'dotenv/config';

const response = await fetch(`http://localhost:${process.env.INGEST_PORT || 3000}/events`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${process.env.INGEST_TOKEN}` },
  body: JSON.stringify({
    tenant_id: process.env.TEST_TENANT_ID || 'tenant_a',
    type: 'order.created',
    payload: { orderId: 4471, amount: 29.99 },
  }),
});
console.log(response.status, await response.json());
