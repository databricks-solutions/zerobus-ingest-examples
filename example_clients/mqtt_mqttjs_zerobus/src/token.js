'use strict';

// Requests a table-scoped OAuth token for Zerobus Ingest with the service principal
// client credentials flow. The token is only returned to the caller. Never log it.

function authorizationDetails(tableName) {
  const [catalog, schema] = tableName.split('.');
  return [
    {
      type: 'unity_catalog_privileges',
      privileges: ['USE CATALOG'],
      object_type: 'CATALOG',
      object_full_path: catalog,
    },
    {
      type: 'unity_catalog_privileges',
      privileges: ['USE SCHEMA'],
      object_type: 'SCHEMA',
      object_full_path: `${catalog}.${schema}`,
    },
    {
      type: 'unity_catalog_privileges',
      privileges: ['SELECT', 'MODIFY'],
      object_type: 'TABLE',
      object_full_path: tableName,
    },
  ];
}

async function fetchZerobusToken(config, fetchImpl = fetch) {
  const basic = Buffer.from(`${config.clientId}:${config.clientSecret}`).toString('base64');
  const body = new URLSearchParams({
    grant_type: 'client_credentials',
    scope: 'all-apis',
    resource: `api://databricks/workspaces/${config.workspaceId}/zerobusDirectWriteApi`,
    authorization_details: JSON.stringify(authorizationDetails(config.tableName)),
  });
  const response = await fetchImpl(`${config.workspaceUrl}/oidc/v1/token`, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${basic}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body,
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) {
    throw new Error(`Token request failed with HTTP ${response.status}`);
  }
  const json = await response.json();
  return json.access_token;
}

module.exports = { authorizationDetails, fetchZerobusToken };
