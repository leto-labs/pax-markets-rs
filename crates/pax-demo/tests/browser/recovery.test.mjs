import test from 'node:test';
import assert from 'node:assert/strict';
import { recoveryStatus } from '../../src/presentation/assets/recovery.mjs';

function report() {
    return {
        session: 'test', published_ns: 70e9, transport_connected: true,
        view: {
            mode: 'live', connected: true,
            last_rejection: {
                id: '16:354013', observed_ns: 10e9, market: '1', sequence: '1814864',
                reason: 'post-event top of book mismatch',
            },
            markets: [{ status: 'live', generation: '17', sequence: '1846626', trace_id: '17:354014' }],
        },
    };
}

test('a later committed snapshot makes the serialized rejection historical', () => {
    const status = recoveryStatus(report());
    assert.equal(status.state, 'recovered');
    assert.match(status.summary, /book resynchronized/);
    assert.match(status.detail, /Generation 16, message 354013, market 1, sequence 1814864/);
    assert.match(status.detail, /post-event top of book mismatch/);
    assert.doesNotMatch(status.detail, /undefined/);
});

test('all selected books must be synchronized after the rejection', () => {
    for (const change of [
        r => r.transport_connected = false,
        r => r.view.connected = false,
        r => r.view.markets = [],
        r => r.view.markets[0].status = 'awaiting snapshot',
        r => r.view.markets[0].status = 'invalid',
        r => r.view.markets[0].sequence = null,
        r => r.view.markets[0].trace_id = null,
        r => r.view.markets[0].generation = '18',
        r => r.view.markets.push({status:'live',generation:'16',sequence:'1814863',trace_id:'16:354012'}),
    ]) {
        const r = report(); change(r);
        assert.equal(recoveryStatus(r).state, 'pending');
    }
});

test('same-generation recovery requires an exact later commit, even above JS integer precision', () => {
    const r = report();
    r.view.last_rejection.id = '16:9007199254740992';
    const book = r.view.markets[0]; book.generation = '16';
    for (const message of ['9007199254740991', '9007199254740992']) {
        book.trace_id = `16:${message}`;
        assert.equal(recoveryStatus(r).state, 'pending');
    }
    book.trace_id = '16:9007199254740993';
    assert.equal(recoveryStatus(r).state, 'recovered');
});

test('missing or malformed identity cannot claim recovery or invent ID fields', () => {
    for (const id of [null, {}, '16', '16:1e3', '-1:2']) {
        const r = report(); r.view.last_rejection.id = id;
        const status = recoveryStatus(r);
        assert.equal(status.state, 'pending');
        assert.match(status.detail, /Input ID unavailable/);
        assert.doesNotMatch(status.detail, /undefined/);
    }
});

test('replay retains the incident without claiming live recovery or a wall-clock age', () => {
    const r = report(); r.view.mode = 'capture replay';
    const status = recoveryStatus(r);
    assert.equal(status.state, 'recorded');
    assert.doesNotMatch(status.detail, /ago/);
});

test('no rejection needs no recovery banner', () => {
    const r = report(); r.view.last_rejection = null;
    assert.equal(recoveryStatus(r), null);
});
