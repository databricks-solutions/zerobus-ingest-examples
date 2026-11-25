'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { authorizationDetails, fetchZerobusToken } = require('../src/token');

test('authorization_details scopes the token to the catalog, schema and table', () => {
  const details = authorizationDetails('main.default.dice_rolls');
  assert.deepEqual(
    details.map((d) => [d.object_type, d.object_full_path, d.privileges]),
    [
      ['CATALOG', 'main', ['USE CATALOG']],
      ['SCHEMA', 'main.default', ['USE SCHEMA']],
      ['TABLE', 'main.default.dice_rolls', ['SELECT', 'MODIFY']],
    ],
  );
});

test('fetchZerobusToken sends client credentials and returns the access token', async () => {
  let seen;
  const fakeFetch = async (url, init) => {
    seen = { url, init };
    return { ok: true, json: async () => ({ access_token: 'fake-token' }) };
  };
  const token = await fetchZerobusToken(
    {
      workspaceUrl: 'https://example.invalid',
      workspaceId: '123',
      clientId: 'id',
      clientSecret: 'secret',
      tableName: 'main.default.dice_rolls',
    },
    fakeFetch,
  );
  assert.equal(token, 'fake-token');
  assert.equal(seen.url, 'https://example.invalid/oidc/v1/token');
  assert.equal(seen.init.headers.Authorization, `Basic ${Buffer.from('id:secret').toString('base64')}`);
  const body = new URLSearchParams(seen.init.body);
  assert.equal(body.get('resource'), 'api://databricks/workspaces/123/zerobusDirectWriteApi');
});

test('fetchZerobusToken reports the HTTP status without the response body', async () => {
  const fakeFetch = async () => ({ ok: false, status: 401 });
  await assert.rejects(
    fetchZerobusToken(
      { workspaceUrl: 'https://x', workspaceId: '1', clientId: 'a', clientSecret: 'b', tableName: 'c.d.e' },
      fakeFetch,
    ),
    /HTTP 401/,
  );
});
