// The delivery connection's own DNS lookup refuses internal addresses (no rebinding
// window). Needs working DNS for the public-name case.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { safeLookup } from '../lib/deliver.js';

const lookup = (host, options = {}) => new Promise((resolve, reject) =>
  safeLookup(host, options, (err, address) => (err ? reject(err) : resolve(address))));

test('names resolving to internal addresses are refused at connect time', async () => {
  await assert.rejects(lookup('localhost'), /blocked/);
  await assert.rejects(lookup('localhost', { all: true }), /blocked/); // the form Node uses when racing IPv4/IPv6
});

test('public names resolve normally, in both callback shapes', async () => {
  assert.ok(typeof (await lookup('example.com')) === 'string');
  assert.ok(Array.isArray(await lookup('example.com', { all: true })));
});
