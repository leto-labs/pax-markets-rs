import test from 'node:test';
import assert from 'node:assert/strict';
import {Poller, Freshness} from '../../src/presentation/assets/polling.mjs';

test('quiet books do not imply an old display; frozen publications do', () => {
  const freshness = new Freshness();
  for (let i = 0; i < 200; i++) freshness.observe('session', String(i), i * 250);
  assert.equal(freshness.status(50000, true).stale, false);
  freshness.observe('session', '199', 54000);
  assert.equal(freshness.status(54000, true).stale, true);
  assert.equal(freshness.status(54000, false).stale, false); // static replay
  freshness.observe('restarted', '0', 55000);
  assert.equal(freshness.status(55000, true).stale, false);
  freshness.fail();
  assert.equal(freshness.status(55000, true).stale, true);
  freshness.observe('restarted', '1', 55250);
  assert.equal(freshness.status(55250, true).stale, false);
  assert.equal(freshness.status(59000, true).stale, true);
});

test('suspended late replies are ignored; resume refreshes immediately and stays single-flight', async () => {
  const requests = [], received = [], errors = [];
  const poller = new Poller({
    load: signal => new Promise(resolve => requests.push({signal, resolve})),
    receive: value => received.push(value), fail: error => errors.push(error),
    interval: () => 60000,
  });
  poller.resume();
  assert.equal(requests.length, 1);
  poller.suspend();
  assert.equal(requests[0].signal.aborted, true);
  poller.resume();
  poller.resume();
  assert.equal(requests.length, 1);
  requests[0].resolve('old');
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(received, []);
  assert.equal(requests.length, 2);
  requests[1].resolve('fresh');
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(received, ['fresh']);
  poller.suspend();
  poller.resume();
  assert.equal(requests.length, 3);
  poller.suspend();
  requests[2].resolve('hidden');
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(received, ['fresh']);
  assert.deepEqual(errors, []);
});
