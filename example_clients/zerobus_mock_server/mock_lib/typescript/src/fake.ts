// In-process fake of the Zerobus TypeScript SDK's ZerobusSdk and ZerobusStream, for unit tests.
// Nothing is sent anywhere. Node strips the types at run time, so there is no build step.
//
//   const mock = new FakeMock();
//   const sdk = new FakeZerobusSdk(mock);
//   const stream = await sdk.createStream({ tableName: 'main.default.events' }, 'id', 'secret');
//   mock.failNext('schema-mismatch', { after: 2 });
//
// The table below is a copy of ../scenarios.json. test/fake.test.ts checks the copy against that file.

const TOKEN_EXPIRED = 'token-expired';

interface Spec {
  where: 'create' | 'ingest' | 'flush' | null;
  text: string;
  retriedBySdk: boolean;
}

export const SCENARIOS: Readonly<Record<string, Spec>> = {
  'auth-failure': { where: 'create', text: 'invalid_client: token endpoint rejected the client', retriedBySdk: false },
  'table-not-found': { where: 'create', text: 'table cannot be found', retriedBySdk: false },
  'schema-mismatch': { where: 'ingest', text: 'record does not fit the table schema', retriedBySdk: false },
  'record-too-large': { where: 'ingest', text: 'record is over the maximum message size', retriedBySdk: false },
  'server-unavailable': { where: 'create', text: 'UNAVAILABLE: retries exhausted', retriedBySdk: true },
  'stream-abort-retryable': { where: null, text: '', retriedBySdk: true },
  'stream-abort-fatal': { where: 'ingest', text: 'terminal stream error: INVALID_ARGUMENT', retriedBySdk: false },
  'graceful-close': { where: null, text: '', retriedBySdk: true },
  'no-acks': { where: 'flush', text: 'flush timed out waiting for acknowledgements', retriedBySdk: false },
  'schema-change-while-open': { where: null, text: '', retriedBySdk: false },
  [TOKEN_EXPIRED]: { where: 'ingest', text: 'token has expired', retriedBySdk: false },
};

/** The error the fake throws. The SDK throws a plain Error, so this is one too, with retriedBySdk added. */
export type ZerobusFailure = Error & { retriedBySdk: boolean };

function requireScenario(name: string): void {
  if (!Object.hasOwn(SCENARIOS, name)) {
    throw new Error(`unknown scenario: ${name}`);
  }
}

function failure(name: string): ZerobusFailure {
  const spec = SCENARIOS[name];
  return Object.assign(new Error(spec.text), { retriedBySdk: spec.retriedBySdk });
}

export interface TableProperties {
  tableName: string;
}

interface Budget {
  after: number;
  left: number;
}

/** Scenario controls. Same names as the Python lib and the server's control API. */
export class FakeMock {
  private budgets = new Map<string, Budget>();
  private held: string | null = null;
  private expiries = 0;

  /** Fails the next `count` matching calls after skipping `after` of them. */
  failNext(scenario: string, opts: { count?: number; after?: number } = {}): void {
    requireScenario(scenario);
    const count = opts.count ?? 1;
    const after = opts.after ?? 0;
    if (count < 1 || after < 0) {
      throw new RangeError('count must be >= 1 and after >= 0');
    }
    if (scenario === TOKEN_EXPIRED) {
      if (after > 0) {
        throw new RangeError('token-expired takes no after; use expireToken');
      }
      this.expiries += count;
      return;
    }
    this.budgets.set(scenario, { after, left: count });
  }

  /** Holds a scenario: every matching call fails until clearScenario(). */
  setScenario(scenario: string): void {
    requireScenario(scenario);
    if (scenario === TOKEN_EXPIRED) {
      throw new Error('token-expired is one-shot: use failNext or expireToken');
    }
    this.held = scenario;
  }

  clearScenario(): void {
    this.held = null;
  }

  /** The next authenticated call finds its token expired: create refreshes it, an ingest fails. */
  expireToken(count = 1): void {
    this.expiries += count;
  }

  /** Clears fail-next budgets, the held scenario and the expiry budget. */
  reset(): void {
    this.budgets.clear();
    this.held = null;
    this.expiries = 0;
  }

  /** @internal */
  isHeld(scenario: string): boolean {
    return this.held === scenario;
  }

  /** @internal A held scenario fires on every matching call; otherwise a fail-next budget may. */
  fire(scenario: string): boolean {
    return this.isHeld(scenario) || this.take(scenario);
  }

  /** @internal True when a fail-next budget fires on this matching call. */
  take(scenario: string): boolean {
    const b = this.budgets.get(scenario);
    if (!b) return false;
    if (b.after > 0) {
      b.after--;
      return false;
    }
    if (b.left <= 0) return false;
    b.left--;
    return true;
  }

  /** @internal */
  takeExpiry(): boolean {
    if (this.expiries > 0) {
      this.expiries--;
      return true;
    }
    return false;
  }
}

/**
 * Stream stand-in. `ingested()` is every record the stream accepted (it returned an offset); `stored()`
 * is what landed in the table, so a retried abort shows up twice; `fallback()` holds rows routed away
 * from the table by schema-change-while-open. A record is acked when ingested, unless no-acks is held.
 */
export class FakeStream {
  readonly tableName: string;
  private readonly mock: FakeMock;
  private accepted: Array<{ offset: bigint; payload: unknown }> = [];
  private storedRows: unknown[] = [];
  private fallbackRows: unknown[] = [];
  private nextOffset = 0n;
  private ackedThrough = -1n;
  private terminal: string | null = null;
  private closed = false;

  constructor(mock: FakeMock, tableName: string) {
    this.mock = mock;
    this.tableName = tableName;
  }

  async ingestRecordOffset(payload: unknown): Promise<bigint> {
    return this.ingest(payload);
  }

  async ingestRecordsOffset(payloads: unknown[]): Promise<bigint | null> {
    let last: bigint | null = null;
    for (const p of payloads) {
      last = this.ingest(p);
    }
    return last;
  }

  async waitForOffset(offset: bigint): Promise<void> {
    this.checkTerminal();
    if (this.mock.take('no-acks') || (this.mock.isHeld('no-acks') && offset > this.ackedThrough)) {
      throw failure('no-acks');
    }
    if (offset > this.ackedThrough) this.ackedThrough = offset;
  }

  async flush(): Promise<void> {
    this.checkTerminal();
    if (this.mock.take('no-acks')) {
      throw failure('no-acks');
    }
    if (this.mock.isHeld('no-acks')) {
      if (this.ackedThrough < this.nextOffset - 1n) throw failure('no-acks');
      return;
    }
    this.ackedThrough = this.nextOffset - 1n;
  }

  /** Accepted records that have not been acknowledged, in order. */
  async getUnackedRecords(): Promise<unknown[]> {
    return this.accepted.filter((e) => e.offset > this.ackedThrough).map((e) => e.payload);
  }

  async close(): Promise<void> {
    this.closed = true;
  }

  ingested(): unknown[] {
    return this.accepted.map((e) => e.payload);
  }

  stored(): unknown[] {
    return [...this.storedRows];
  }

  fallback(): unknown[] {
    return [...this.fallbackRows];
  }

  private ingest(payload: unknown): bigint {
    if (this.closed) throw new Error('stream is closed');
    this.checkTerminal();
    if (this.mock.takeExpiry()) {
      this.terminal = TOKEN_EXPIRED;
      throw failure(TOKEN_EXPIRED);
    }
    if (this.mock.fire('schema-mismatch')) throw failure('schema-mismatch');
    if (this.mock.fire('record-too-large')) throw failure('record-too-large');
    if (this.mock.fire('stream-abort-fatal')) {
      this.terminal = 'stream-abort-fatal';
      throw failure('stream-abort-fatal');
    }
    const offset = this.nextOffset++;
    this.accepted.push({ offset, payload });
    if (!this.mock.isHeld('no-acks')) this.ackedThrough = offset;
    if (this.mock.fire('schema-change-while-open')) {
      this.fallbackRows.push(payload);
      return offset;
    }
    this.storedRows.push(payload);
    if (this.mock.fire('stream-abort-retryable')) {
      this.storedRows.push(payload); // the aborted copy was stored unacked; the SDK's resend stores it again
    }
    return offset;
  }

  private checkTerminal(): void {
    if (this.terminal !== null) throw failure(this.terminal);
  }
}

/** Same entry point as the SDK's ZerobusSdk.createStream(tableProperties, clientId, clientSecret, options). */
export class FakeZerobusSdk {
  private readonly mock: FakeMock;

  constructor(mock?: FakeMock) {
    this.mock = mock ?? new FakeMock();
  }

  async createStream(tableProperties: TableProperties, _clientId: string, _clientSecret: string): Promise<FakeStream> {
    if (this.mock.fire('auth-failure')) throw failure('auth-failure');
    if (this.mock.fire('table-not-found')) throw failure('table-not-found');
    if (this.mock.fire('server-unavailable')) throw failure('server-unavailable');
    // An expired token is refreshed during create, so the caller sees success.
    this.mock.takeExpiry();
    return new FakeStream(this.mock, tableProperties.tableName);
  }
}
