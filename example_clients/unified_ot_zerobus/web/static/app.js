/* Unified Connector Web UI (vanilla JS) */

// Global state for current user and permissions
let currentUser = null;
let userPermissions = [];

function $(sel) {
  const el = document.querySelector(sel);
  if (!el) throw new Error(`Missing element: ${sel}`);
  return el;
}

function safeJsonStringify(v) {
  try {
    return JSON.stringify(v, null, 2);
  } catch {
    return String(v);
  }
}

let toastTimer = null;
function toast(message, kind = "ok") {
  const el = $("#toast");
  el.className = `toast show ${kind}`;
  el.textContent = message;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    el.className = "toast";
  }, 2600);
}

async function apiFetch(path, options = {}) {
  const resp = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    credentials: "include", // Important for session cookies
    ...options,
  });

  // Handle 401 Unauthorized - throw error but don't redirect
  // (let checkAuth() handle the redirect logic based on auth_enabled)
  if (resp.status === 401) {
    throw new Error("Authentication required");
  }

  const contentType = resp.headers.get("content-type") || "";
  let body = null;
  if (contentType.includes("application/json")) body = await resp.json();
  else body = await resp.text();

  if (!resp.ok) {
    const msg =
      (body && body.message) ||
      (typeof body === "string" ? body : null) ||
      `HTTP ${resp.status}`;
    throw new Error(msg);
  }
  return body;
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setKVs(containerSel, obj) {
  const container = $(containerSel);
  container.innerHTML = "";
  if (!obj || typeof obj !== "object") {
    container.innerHTML = `<div class="muted">No data</div>`;
    return;
  }

  const entries = Object.entries(obj);
  if (entries.length === 0) {
    container.innerHTML = `<div class="muted">No data</div>`;
    return;
  }

  for (const [k, v] of entries) {
    const div = document.createElement("div");
    div.className = "kv";
    div.innerHTML = `<div class="k">${escapeHtml(k)}</div><div class="v">${escapeHtml(
      typeof v === "string" ? v : safeJsonStringify(v)
    )}</div>`;
    container.appendChild(div);
  }
}

function setBadge(sel, value, kind = "ok") {
  const el = $(sel);
  el.className = `pill ${kind}`;
  el.innerHTML = `<strong>${escapeHtml(value)}</strong>`;
}

function buildFqn(catalog, schema, table) {
  const c = (catalog || "").trim();
  const s = (schema || "").trim();
  const t = (table || "").trim();
  return [c, s, t].filter(Boolean).join(".");
}

function parseFqn(fqn) {
  const parts = (fqn || "").split(".").map((p) => p.trim()).filter(Boolean);
  if (parts.length !== 3) return null;
  return { catalog: parts[0], schema: parts[1], table: parts[2] };
}

function normalizeZerobusConfigForForm(cfg) {
  const catalog = cfg?.default_target?.catalog || "";
  const schema = cfg?.default_target?.schema || "";
  const table = cfg?.default_target?.table || "";

  return {
    enabled: Boolean(cfg?.enabled),
    workspace_host: cfg?.workspace_host || cfg?.default_target?.workspace_host || "",
    zerobus_endpoint: cfg?.zerobus_endpoint || "",
    table_fqn: buildFqn(catalog, schema, table),
    auth: {
      client_id: cfg?.auth?.client_id || "",
      client_secret: "",
      has_secret: cfg?.auth?.client_secret === "***",
    },
  };
}

async function refreshStatus() {
  const status = await apiFetch("/api/status");
  setKVs("#statusKVs", status);

  const zerobusConnected =
    status?.zerobus_connected ?? status?.zerobus?.connected ?? status?.zerobusConnected;
  if (typeof zerobusConnected === "boolean") {
    setBadge(
      "#badgeZerobus",
      zerobusConnected ? "ZeroBus: connected" : "ZeroBus: disconnected",
      zerobusConnected ? "ok" : "bad"
    );
  } else {
    setBadge("#badgeZerobus", "ZeroBus: unknown", "warn");
  }

  const configured = status?.configured_sources;
  const active = status?.active_sources;
  if (typeof configured === "number" && typeof active === "number") {
    setBadge("#badgeSources", `Sources: ${configured} configured (${active} active)`, active > 0 ? "ok" : "warn");
  } else if (typeof configured === "number") {
    setBadge("#badgeSources", `Sources: ${configured} configured`, "ok");
  } else {
    setBadge("#badgeSources", "Sources: unknown", "warn");
  }

  const discoveryCount = status?.discovery_count;
  if (typeof discoveryCount === "number") {
    setBadge("#badgeDiscovery", `Discovery: ${discoveryCount} found`, discoveryCount > 0 ? "ok" : "warn");
  } else {
    const discoveryOn = status?.discovery?.enabled ?? status?.discovery_enabled;
    if (typeof discoveryOn === "boolean")
      setBadge(
        "#badgeDiscovery",
        discoveryOn ? "Discovery: on" : "Discovery: off",
        discoveryOn ? "ok" : "warn"
      );
    else setBadge("#badgeDiscovery", "Discovery: unknown", "warn");
  }

}

async function refreshMetrics() {
  const metrics = await apiFetch("/api/metrics");
  setKVs("#metricsKVs", metrics);
}

async function refreshPipeline() {
  try {
    const [pipelineData, metricsData] = await Promise.all([
      apiFetch("/api/diagnostics/pipeline"),
      apiFetch("/api/metrics")
    ]);

    // Update vendor format badges
    const vendorFormats = pipelineData.vendor_format_summary || {};
    $("#vendorKepware").innerHTML = `<strong>Kepware: ${vendorFormats.kepware || 0}</strong>`;
    $("#vendorSparkplug").innerHTML = `<strong>Sparkplug B: ${vendorFormats.sparkplug_b || 0}</strong>`;
    $("#vendorHoneywell").innerHTML = `<strong>Honeywell: ${vendorFormats.honeywell || 0}</strong>`;
    $("#vendorGeneric").innerHTML = `<strong>Generic: ${vendorFormats.generic || 0}</strong>`;

    // Update normalization status
    const normEnabled = pipelineData.normalization_enabled || false;
    $("#pipelineNormStatus").textContent = `ISA-95 normalization: ${normEnabled ? 'ENABLED' : 'DISABLED'}`;
    $("#pipelineNormStatus").style.color = normEnabled ? 'var(--good)' : 'var(--warning)';

    // Get pipeline container
    const pipelineContainer = document.getElementById('pipelineFlowContainer');
    if (!pipelineContainer) return;

    // Clear existing pipeline
    pipelineContainer.innerHTML = '';

    // Get protocol-vendor breakdown from metrics
    const protocolVendorBreakdown = metricsData?.bridge?.protocol_vendor_breakdown || {};

    // Render protocol-vendor breakdown section
    renderProtocolVendorBreakdown(protocolVendorBreakdown);

    // Reorganize vendor pipelines by protocol-vendor combination
    const vendorPipelines = pipelineData.vendor_pipelines || {};
    const protocolVendorPipelines = organizeByProtocolVendor(vendorPipelines);

    console.log('Protocol-Vendor Pipelines:', Object.keys(protocolVendorPipelines));

    // Render filter controls
    renderPipelineFilters(protocolVendorPipelines);

    // Get selected filters
    const selectedProtocol = window.pipelineFilterProtocol || 'all';
    const selectedVendor = window.pipelineFilterVendor || 'all';

    // Render per protocol-vendor pipelines based on filters
    for (const [key, data] of Object.entries(protocolVendorPipelines)) {
      // Apply filters
      if (selectedProtocol !== 'all' && data.protocol !== selectedProtocol) continue;
      if (selectedVendor !== 'all' && data.vendor !== selectedVendor) continue;

      console.log(`Rendering ${key}:`, data.protocol, '→', data.vendor, `(${data.stages.length} stages)`);
      renderProtocolVendorPipeline(key, data.protocol, data.vendor, data.stages, data.recordCount);
    }
  } catch (e) {
    console.error("Failed to refresh pipeline diagnostics:", e);
    toast("Failed to load pipeline diagnostics", "bad");
  }
}

/**
 * Render filter controls for protocol and vendor selection
 */
function renderPipelineFilters(protocolVendorPipelines) {
  const pipelineContainer = document.getElementById('pipelineFlowContainer');
  if (!pipelineContainer) return;

  // Extract unique protocols and vendors
  const protocols = new Set();
  const vendors = new Set();

  for (const data of Object.values(protocolVendorPipelines)) {
    protocols.add(data.protocol);
    vendors.add(data.vendor);
  }

  // Protocol display names
  const protocolNames = {
    opcua: 'OPC UA',
    mqtt: 'MQTT',
    modbus: 'Modbus',
    unknown: 'Unknown'
  };

  // Vendor display names
  const vendorNames = {
    kepware: 'Kepware',
    sparkplug_b: 'Sparkplug B',
    honeywell: 'Honeywell',
    opcua: 'OPC UA',
    modbus: 'Modbus',
    generic: 'Generic',
    unknown: 'Unknown'
  };

  // Create filter section
  const filterSection = document.createElement('div');
  filterSection.className = 'pipeline-filters';
  filterSection.style.cssText = 'margin-bottom: 20px; padding: 16px; background: var(--bg-secondary); border-radius: 8px; display: flex; gap: 16px; align-items: center;';

  // Filter label
  const filterLabel = document.createElement('div');
  filterLabel.style.cssText = 'font-size: 14px; font-weight: 700; color: var(--text);';
  filterLabel.textContent = 'Filter Pipeline:';
  filterSection.appendChild(filterLabel);

  // Protocol filter
  const protocolFilterContainer = document.createElement('div');
  protocolFilterContainer.style.cssText = 'display: flex; flex-direction: column; gap: 4px;';

  const protocolLabel = document.createElement('label');
  protocolLabel.style.cssText = 'font-size: 11px; color: var(--muted); font-weight: 600;';
  protocolLabel.textContent = 'Protocol:';
  protocolFilterContainer.appendChild(protocolLabel);

  const protocolSelect = document.createElement('select');
  protocolSelect.id = 'pipelineProtocolFilter';
  protocolSelect.style.cssText = 'padding: 6px 12px; border: 1px solid var(--border-panel); border-radius: 4px; background: var(--bg-secondary); color: var(--text-primary); font-size: 13px; cursor: pointer; min-width: 150px;';

  // Add "All" option
  const allProtocolOption = document.createElement('option');
  allProtocolOption.value = 'all';
  allProtocolOption.textContent = 'All Protocols';
  protocolSelect.appendChild(allProtocolOption);

  // Add protocol options
  for (const protocol of Array.from(protocols).sort()) {
    if (protocol === 'unknown') continue;
    const option = document.createElement('option');
    option.value = protocol;
    option.textContent = protocolNames[protocol] || protocol;
    protocolSelect.appendChild(option);
  }

  protocolSelect.addEventListener('change', () => {
    window.pipelineFilterProtocol = protocolSelect.value;
    refreshPipeline();
  });

  protocolFilterContainer.appendChild(protocolSelect);
  filterSection.appendChild(protocolFilterContainer);

  // Vendor filter
  const vendorFilterContainer = document.createElement('div');
  vendorFilterContainer.style.cssText = 'display: flex; flex-direction: column; gap: 4px;';

  const vendorLabel = document.createElement('label');
  vendorLabel.style.cssText = 'font-size: 11px; color: var(--muted); font-weight: 600;';
  vendorLabel.textContent = 'Vendor Format:';
  vendorFilterContainer.appendChild(vendorLabel);

  const vendorSelect = document.createElement('select');
  vendorSelect.id = 'pipelineVendorFilter';
  vendorSelect.style.cssText = 'padding: 6px 12px; border: 1px solid var(--border-panel); border-radius: 4px; background: var(--bg-secondary); color: var(--text-primary); font-size: 13px; cursor: pointer; min-width: 150px;';

  // Add "All" option
  const allVendorOption = document.createElement('option');
  allVendorOption.value = 'all';
  allVendorOption.textContent = 'All Vendors';
  vendorSelect.appendChild(allVendorOption);

  // Add vendor options
  for (const vendor of Array.from(vendors).sort()) {
    if (vendor === 'unknown') continue;
    const option = document.createElement('option');
    option.value = vendor;
    option.textContent = vendorNames[vendor] || vendor;
    vendorSelect.appendChild(option);
  }

  vendorSelect.addEventListener('change', () => {
    window.pipelineFilterVendor = vendorSelect.value;
    refreshPipeline();
  });

  vendorFilterContainer.appendChild(vendorSelect);
  filterSection.appendChild(vendorFilterContainer);

  // Restore selected values
  if (window.pipelineFilterProtocol) {
    protocolSelect.value = window.pipelineFilterProtocol;
  }
  if (window.pipelineFilterVendor) {
    vendorSelect.value = window.pipelineFilterVendor;
  }

  pipelineContainer.appendChild(filterSection);
}

/**
 * Reorganize vendor pipelines by protocol-vendor combination
 * Takes vendor_pipelines structure and splits by protocol found in samples
 */
function organizeByProtocolVendor(vendorPipelines) {
  const protocolVendorPipelines = {};

  for (const [vendorKey, vendorData] of Object.entries(vendorPipelines)) {
    const stages = vendorData.pipeline_stages || [];
    if (!stages || stages.length === 0) continue;

    // Group samples by protocol across all stages
    const protocolGroups = {};
    const sourceNameToProtocol = {}; // Map source_name to protocol for later stages

    // First stage has the protocol field
    const firstStage = stages[0];
    const firstStageSamples = firstStage.samples || [];

    for (const sample of firstStageSamples) {
      const protocol = sample.protocol || sample.protocol_type || 'unknown';
      const sourceName = sample.source_name || 'unknown';

      // Remember which protocol this source_name belongs to
      sourceNameToProtocol[sourceName] = protocol;

      if (!protocolGroups[protocol]) {
        protocolGroups[protocol] = {
          protocol,
          vendor: vendorKey,
          stages: [],
          recordCount: 0,
          samplesByStage: {}
        };
      }
    }

    // Now organize samples from all stages by protocol
    for (let stageIdx = 0; stageIdx < stages.length; stageIdx++) {
      const stage = stages[stageIdx];
      const stageName = stage.stage || stage.name || stage.stage_name || `Stage ${stageIdx + 1}`;
      const samples = stage.samples || [];

      // Initialize stage structure for all protocols (even if no samples yet)
      for (const protocol of Object.keys(protocolGroups)) {
        if (!protocolGroups[protocol].samplesByStage[stageIdx]) {
          protocolGroups[protocol].samplesByStage[stageIdx] = {
            name: stageName,
            description: stage.description || '',
            samples: []
          };
        }
      }

      // Group samples by protocol
      for (const sample of samples) {
        // Try to get protocol from sample first, then from source_name mapping
        let protocol = sample.protocol || sample.protocol_type;

        if (!protocol && sample.source_name) {
          protocol = sourceNameToProtocol[sample.source_name];
        }

        protocol = protocol || 'unknown';

        if (protocolGroups[protocol]) {
          protocolGroups[protocol].samplesByStage[stageIdx].samples.push(sample);
          protocolGroups[protocol].recordCount++;
        }
      }
    }

    // Convert samplesByStage to stages array
    for (const [protocol, group] of Object.entries(protocolGroups)) {
      group.stages = Object.values(group.samplesByStage);
      delete group.samplesByStage;

      const key = `${protocol}_${vendorKey}`;
      protocolVendorPipelines[key] = group;
    }
  }

  return protocolVendorPipelines;
}

/**
 * Render a pipeline flow for a specific protocol-vendor combination
 */
function renderProtocolVendorPipeline(key, protocol, vendor, stages, recordCount) {
  const pipelineContainer = document.getElementById('pipelineFlowContainer');
  if (!pipelineContainer) return;

  // Display names
  const protocolNames = {
    opcua: 'OPC UA',
    mqtt: 'MQTT',
    modbus: 'Modbus',
    unknown: 'Unknown'
  };

  const vendorNames = {
    kepware: 'Kepware',
    sparkplug_b: 'Sparkplug B',
    honeywell: 'Honeywell',
    opcua: 'OPC UA',
    modbus: 'Modbus',
    generic: 'Generic',
    unknown: 'Unknown'
  };

  const protocolDisplay = protocolNames[protocol] || protocol;
  const vendorDisplay = vendorNames[vendor] || vendor;

  // Create pipeline section
  const pipelineSection = document.createElement('div');
  pipelineSection.className = 'protocol-vendor-pipeline-section';
  pipelineSection.style.marginBottom = '24px';

  // Pipeline header showing protocol → vendor → ZeroBus
  const pipelineHeader = document.createElement('div');
  pipelineHeader.style.cssText = 'display: flex; align-items: center; gap: 12px; margin-bottom: 12px; padding: 12px; background: var(--bg-secondary); border-radius: 6px;';

  pipelineHeader.innerHTML = `
    <span class="pill secondary" style="font-size: 12px; padding: 6px 12px;">${escapeHtml(protocolDisplay)}</span>
    <span style="font-size: 16px; color: var(--text-muted);">→</span>
    <span class="pill secondary" style="font-size: 12px; padding: 6px 12px;">${escapeHtml(vendorDisplay)}</span>
    <span style="font-size: 16px; color: var(--text-muted);">→</span>
    <span class="pill secondary" style="font-size: 12px; padding: 6px 12px;">ISA-95</span>
    <span style="font-size: 16px; color: var(--text-muted);">→</span>
    <span class="pill primary" style="font-size: 12px; padding: 6px 12px;">ZeroBus</span>
    <span style="margin-left: auto; font-size: 12px; color: var(--text-secondary); font-family: var(--font-data);">${recordCount} records</span>
  `;
  pipelineSection.appendChild(pipelineHeader);

  // Pipeline flow container
  const pipelineFlow = document.createElement('div');
  pipelineFlow.className = 'pipeline-flow';
  pipelineFlow.style.cssText = 'display: flex; align-items: flex-start; gap: 12px; overflow-x: auto; padding: 4px;';

  // Stage display names
  const stageDisplayNames = {
    'raw_protocol': 'Raw Protocol',
    'after_vendor_detection': 'Vendor Detection',
    'after_normalization': 'ISA-95 Normalization',
    'zerobus_batch': 'ZeroBus Batch'
  };

  // Render each stage
  for (let i = 0; i < stages.length; i++) {
    const stage = stages[i];
    const stageName = stage.stage || stage.name || stage.stage_name || `Stage ${i + 1}`;
    const stageDisplay = stageDisplayNames[stageName] || stageName;

    // Create stage element
    const stageEl = document.createElement('div');
    stageEl.className = 'pipeline-stage';
    stageEl.style.cssText = 'flex: 1; min-width: 260px; background: var(--bg-secondary); border: 1px solid var(--border-panel); border-radius: 8px; padding: 12px;';

    // Stage header
    const stageHeader = document.createElement('div');
    stageHeader.className = 'pipeline-stage-header';
    stageHeader.style.cssText = 'display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;';

    const stageTitle = document.createElement('div');
    stageTitle.className = 'pipeline-stage-title';
    stageTitle.style.cssText = 'font-size: 13px; font-weight: 700; color: var(--bloomberg-orange); text-transform: uppercase;';
    stageTitle.textContent = `${i + 1}. ${stageDisplay}`;

    const stageCount = document.createElement('div');
    stageCount.className = 'pipeline-stage-count';
    stageCount.style.cssText = 'font-size: 11px; color: var(--text-secondary); font-weight: 600;';
    stageCount.textContent = `${stage.samples?.length || 0} samples`;

    stageHeader.appendChild(stageTitle);
    stageHeader.appendChild(stageCount);
    stageEl.appendChild(stageHeader);

    // Stage description
    const stageDesc = document.createElement('div');
    stageDesc.className = 'pipeline-stage-desc';
    stageDesc.style.cssText = 'font-size: 11px; color: var(--text-secondary); margin-bottom: 12px;';
    stageDesc.textContent = stage.description || '';
    stageEl.appendChild(stageDesc);

    // Stage samples
    const stageSamples = document.createElement('div');
    stageSamples.className = 'pipeline-stage-samples';
    stageSamples.style.cssText = 'display: flex; flex-direction: column; gap: 8px; max-height: 400px; overflow-y: auto;';

    if (!stage.samples || stage.samples.length === 0) {
      stageSamples.innerHTML = '<div style="font-size: 11px; color: var(--text-secondary);">No samples yet</div>';
    } else {
      // Show max 3 samples
      const samplesToShow = stage.samples.slice(0, 3);
      for (const sample of samplesToShow) {
        const sampleDiv = document.createElement('div');
        sampleDiv.className = 'pipeline-sample';
        sampleDiv.style.cssText = 'background: var(--bg-panel); border: 1px solid var(--border-panel); border-left: 3px solid var(--status-idle); border-radius: 4px; padding: 8px; font-size: 11px;';

        const timestamp = sample.timestamp ? new Date(sample.timestamp * 1000).toLocaleTimeString() : 'N/A';

        // Create clean JSON by removing stage and timestamp fields
        const jsonData = {...sample};
        delete jsonData.stage;
        delete jsonData.timestamp;
        delete jsonData.protocol;
        delete jsonData.protocol_type;

        sampleDiv.innerHTML = `
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
            <span style="font-weight: 600; color: var(--status-running);">${escapeHtml(vendorDisplay)}</span>
            <span style="color: var(--text-muted); font-family: var(--font-data);">${escapeHtml(timestamp)}</span>
          </div>
          <pre style="margin: 0; font-size: 10px; color: var(--text-primary); font-family: var(--font-data); white-space: pre-wrap; word-break: break-all; background: rgba(0, 0, 0, 0.3); padding: 8px; border-radius: 3px;">${escapeHtml(JSON.stringify(jsonData, null, 2))}</pre>
        `;

        stageSamples.appendChild(sampleDiv);
      }
    }

    stageEl.appendChild(stageSamples);
    pipelineFlow.appendChild(stageEl);

    // Add arrow between stages (except after last stage)
    if (i < stages.length - 1) {
      const arrow = document.createElement('div');
      arrow.className = 'pipeline-arrow';
      arrow.style.cssText = 'font-size: 24px; color: var(--muted); padding: 80px 0;';
      arrow.textContent = '→';
      pipelineFlow.appendChild(arrow);
    }
  }

  pipelineSection.appendChild(pipelineFlow);
  pipelineContainer.appendChild(pipelineSection);
}

function renderProtocolVendorBreakdown(protocolVendorBreakdown) {
  const pipelineContainer = document.getElementById('pipelineFlowContainer');
  if (!pipelineContainer) return;

  // Protocol display names
  const protocolNames = {
    opcua: 'OPC UA',
    mqtt: 'MQTT',
    modbus: 'Modbus',
    unknown: 'Unknown'
  };

  // Vendor display names
  const vendorNames = {
    kepware: 'Kepware',
    sparkplug_b: 'Sparkplug B',
    honeywell: 'Honeywell',
    opcua: 'OPC UA',
    modbus: 'Modbus',
    generic: 'Generic',
    unknown: 'Unknown'
  };

  // Create breakdown section
  const breakdownSection = document.createElement('div');
  breakdownSection.className = 'protocol-vendor-breakdown';
  breakdownSection.style.cssText = 'margin-bottom: 24px; padding: 16px; background: var(--bg-secondary); border-radius: 8px;';

  // Section header
  const header = document.createElement('div');
  header.style.cssText = 'font-size: 15px; font-weight: 700; color: var(--bloomberg-orange); margin-bottom: 12px; text-transform: uppercase;';
  header.textContent = 'Protocol → Vendor Format Breakdown';
  breakdownSection.appendChild(header);

  // Create grid for protocol-vendor combinations
  const grid = document.createElement('div');
  grid.style.cssText = 'display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 12px;';

  // Iterate through protocols
  for (const [protocol, vendors] of Object.entries(protocolVendorBreakdown)) {
    if (!vendors || typeof vendors !== 'object') continue;

    // Skip unknown if it has no meaningful data
    if (protocol === 'unknown' && Object.keys(vendors).length === 1 && vendors.unknown) {
      continue;
    }

    // Iterate through vendors for this protocol
    for (const [vendor, count] of Object.entries(vendors)) {
      if (count === 0) continue;

      // Create card for this protocol-vendor combo
      const card = document.createElement('div');
      card.style.cssText = 'background: var(--bg-panel); border: 1px solid var(--border-panel); border-radius: 6px; padding: 12px; display: flex; flex-direction: column; gap: 8px;';

      // Protocol → Vendor label
      const label = document.createElement('div');
      label.style.cssText = 'display: flex; align-items: center; gap: 8px; font-size: 13px; font-weight: 600; color: var(--text-primary);';

      // Protocol badge
      const protocolBadge = document.createElement('span');
      protocolBadge.className = 'pill secondary';
      protocolBadge.style.cssText = 'font-size: 11px; padding: 4px 8px;';
      protocolBadge.textContent = protocolNames[protocol] || protocol;

      // Arrow
      const arrow = document.createElement('span');
      arrow.style.cssText = 'color: var(--text-muted); font-size: 16px;';
      arrow.textContent = '→';

      // Vendor badge
      const vendorBadge = document.createElement('span');
      vendorBadge.className = 'pill secondary';
      vendorBadge.style.cssText = 'font-size: 11px; padding: 4px 8px;';
      vendorBadge.textContent = vendorNames[vendor] || vendor;

      label.appendChild(protocolBadge);
      label.appendChild(arrow);
      label.appendChild(vendorBadge);
      card.appendChild(label);

      // Count
      const countDiv = document.createElement('div');
      countDiv.style.cssText = 'font-size: 20px; font-weight: 700; color: var(--status-running); font-family: var(--font-data);';
      countDiv.textContent = count.toLocaleString();
      card.appendChild(countDiv);

      // Description
      const desc = document.createElement('div');
      desc.style.cssText = 'font-size: 11px; color: var(--text-secondary);';
      desc.textContent = 'records';
      card.appendChild(desc);

      grid.appendChild(card);
    }
  }

  breakdownSection.appendChild(grid);
  pipelineContainer.appendChild(breakdownSection);
}

function renderVendorPipeline(vendorKey, vendorName, vendorData) {
  const pipelineContainer = document.getElementById('pipelineFlowContainer');
  if (!pipelineContainer) return;

  const stages = vendorData.pipeline_stages || [];
  const recordCount = vendorData.record_count || 0;

  // Create vendor section
  const vendorSection = document.createElement('div');
  vendorSection.className = 'vendor-pipeline-section';
  vendorSection.style.marginBottom = '24px';

  // Vendor header
  const vendorHeader = document.createElement('div');
  vendorHeader.style.cssText = 'display: flex; align-items: center; gap: 12px; margin-bottom: 12px; padding: 8px 12px; background: var(--bg-secondary); border-radius: 6px;';
  vendorHeader.innerHTML = `
    <span style="font-size: 14px; font-weight: 700; color: var(--text);">${escapeHtml(vendorName)}</span>
    <span style="font-size: 12px; color: var(--muted);">${recordCount} records</span>
  `;
  vendorSection.appendChild(vendorHeader);

  // Pipeline flow container for this vendor
  const pipelineFlow = document.createElement('div');
  pipelineFlow.className = 'pipeline-flow';
  pipelineFlow.style.cssText = 'display: flex; align-items: flex-start; gap: 12px; overflow-x: auto;';

  // Render each stage
  for (let i = 0; i < stages.length; i++) {
    const stage = stages[i];

    // Create stage element
    const stageEl = document.createElement('div');
    stageEl.className = 'pipeline-stage';
    stageEl.style.cssText = 'flex: 1; min-width: 240px; background: var(--bg-secondary); border: 1px solid var(--border-panel); border-radius: 8px; padding: 12px;';

    // Stage header
    const stageHeader = document.createElement('div');
    stageHeader.className = 'pipeline-stage-header';
    stageHeader.style.cssText = 'display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;';

    const stageTitle = document.createElement('div');
    stageTitle.className = 'pipeline-stage-title';
    stageTitle.style.cssText = 'font-size: 13px; font-weight: 700; color: var(--bloomberg-orange); text-transform: uppercase;';
    stageTitle.textContent = `${i + 1}. ${stage.name || stage.stage}`;

    const stageCount = document.createElement('div');
    stageCount.className = 'pipeline-stage-count';
    stageCount.style.cssText = 'font-size: 11px; color: var(--text-secondary); font-weight: 600;';
    stageCount.textContent = `${stage.sample_count || 0} samples`;

    stageHeader.appendChild(stageTitle);
    stageHeader.appendChild(stageCount);
    stageEl.appendChild(stageHeader);

    // Stage description
    const stageDesc = document.createElement('div');
    stageDesc.className = 'pipeline-stage-desc';
    stageDesc.style.cssText = 'font-size: 11px; color: var(--text-secondary); margin-bottom: 12px;';
    stageDesc.textContent = stage.description || '';
    stageEl.appendChild(stageDesc);

    // Stage samples
    const stageSamples = document.createElement('div');
    stageSamples.className = 'pipeline-stage-samples';
    stageSamples.style.cssText = 'display: flex; flex-direction: column; gap: 8px; max-height: 400px; overflow-y: auto;';

    if (!stage.samples || stage.samples.length === 0) {
      stageSamples.innerHTML = '<div style="font-size: 11px; color: var(--text-secondary);">No samples yet</div>';
    } else {
      for (const sample of stage.samples) {
        const sampleDiv = document.createElement('div');
        sampleDiv.className = 'pipeline-sample';
        sampleDiv.style.cssText = 'background: var(--bg-panel); border: 1px solid var(--border-panel); border-left: 3px solid var(--status-idle); border-radius: 4px; padding: 8px; font-size: 11px;';

        const timestamp = sample.timestamp ? new Date(sample.timestamp * 1000).toLocaleTimeString() : 'N/A';

        // Create clean JSON by removing stage and timestamp fields
        const jsonData = {...sample};
        delete jsonData.stage;
        delete jsonData.timestamp;

        sampleDiv.innerHTML = `
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;">
            <span style="font-weight: 600; color: var(--brand);">${escapeHtml(vendorName)}</span>
            <span style="color: var(--muted);">${escapeHtml(timestamp)}</span>
          </div>
          <pre style="margin: 0; font-size: 10px; color: var(--text); white-space: pre-wrap; word-break: break-all;">${escapeHtml(JSON.stringify(jsonData, null, 2))}</pre>
        `;

        stageSamples.appendChild(sampleDiv);
      }
    }

    stageEl.appendChild(stageSamples);
    pipelineFlow.appendChild(stageEl);

    // Add arrow between stages (except after last stage)
    if (i < stages.length - 1) {
      const arrow = document.createElement('div');
      arrow.className = 'pipeline-arrow';
      arrow.style.cssText = 'font-size: 24px; color: var(--muted); padding: 80px 0;';
      arrow.textContent = '→';
      pipelineFlow.appendChild(arrow);
    }
  }

  vendorSection.appendChild(pipelineFlow);
  pipelineContainer.appendChild(vendorSection);
}

function renderPipelineStage(stage) {
  const stageMap = {
    'raw_protocol': 'samplesRaw',
    'after_vendor_detection': 'samplesVendor',
    'after_normalization': 'samplesNormalized',
    'zerobus_batch': 'samplesZerobus'
  };

  const containerId = stageMap[stage.stage];
  if (!containerId) return;

  const container = document.getElementById(containerId);
  if (!container) return;

  // Update sample count
  const stageEl = container.closest('.pipeline-stage');
  const countEl = stageEl?.querySelector('.pipeline-stage-count');
  if (countEl) {
    countEl.textContent = `${stage.sample_count || 0} samples`;
  }

  // Clear previous samples
  container.innerHTML = '';

  if (!stage.samples || stage.samples.length === 0) {
    container.innerHTML = '<div class="muted" style="font-size: 11px;">No samples yet</div>';
    return;
  }

  // Render samples
  for (const sample of stage.samples) {
    const sampleDiv = document.createElement('div');
    sampleDiv.className = 'pipeline-sample';

    const vendor = sample.vendor_format || 'unknown';
    const vendorClass = `vendor-${vendor}`;
    const timestamp = sample.timestamp ? new Date(sample.timestamp * 1000).toLocaleTimeString() : 'N/A';

    // Create clean JSON by removing stage and timestamp fields
    const jsonData = {...sample};
    delete jsonData.stage;
    delete jsonData.timestamp;

    sampleDiv.innerHTML = `
      <div class="pipeline-sample-header">
        <span class="pipeline-sample-vendor ${vendorClass}">${escapeHtml(vendor)}</span>
        <span class="pipeline-sample-time">${escapeHtml(timestamp)}</span>
      </div>
      <div class="pipeline-sample-json">${escapeHtml(JSON.stringify(jsonData, null, 2))}</div>
    `;

    container.appendChild(sampleDiv);
  }
}

async function refreshSources() {
  const [data, status] = await Promise.all([apiFetch("/api/sources"), apiFetch("/api/status")]);
  const sources = Array.isArray(data?.sources) ? data.sources : [];
  const clients = status?.metrics?.clients || {};
  const activeMap = new Set(Object.keys(clients));

  const tbody = $("#sourcesTbody");
  tbody.innerHTML = "";

  if (sources.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="8" class="muted">No sources configured yet.</td>`;
    tbody.appendChild(tr);
    return;
  }

  for (const s of sources) {
    const name = s?.name ?? "";
    const protocol = s?.protocol ?? "";
    const endpoint = s?.endpoint ?? "";
    const enabled = s?.enabled;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><input type="checkbox" data-select="source" data-name="${escapeHtml(name)}" /></td>
      <td><code>${escapeHtml(name)}</code></td>
      <td>${escapeHtml(protocol)}</td>
      <td><code>${escapeHtml(endpoint)}</code></td>
      <td>${activeMap.has(name) ? "<strong>yes</strong>" : "<span class='muted'>no</span>"}</td>
      <td>${typeof enabled === "boolean" ? (enabled ? "true" : "false") : "<span class='muted'>n/a</span>"}</td>
      <td>
        <span class="actions">
        ${activeMap.has(name)
          ? `<button class="btn btn-warning" data-action="stop" data-name="${escapeHtml(name)}">Stop</button>`
          : `<button class="btn btn-good" data-action="start" data-name="${escapeHtml(name)}">Start</button>`
        }
        <button class="btn btn-secondary" data-action="edit" data-name="${escapeHtml(name)}">Edit</button>
        </span>
      </td>
      <td style="text-align:right">
        <button class="iconbtn danger" title="Delete" aria-label="Delete" data-action="delete" data-name="${escapeHtml(name)}">x</button>
      </td>
    `;
    tbody.appendChild(tr);
  }
}

async function refreshDiscovered() {
  const data = await apiFetch(`/api/discovery/servers`);
  const servers = Array.isArray(data?.servers) ? data.servers : [];

  const groups = { opcua: [], mqtt: [], modbus: [] };
  for (const s of servers) {
    const p = (s?.protocol || "").toLowerCase();
    if (p === "opcua") groups.opcua.push(s);
    else if (p === "mqtt") groups.mqtt.push(s);
    else if (p === "modbus") groups.modbus.push(s);
  }

  const renderGroup = (tbodySel, list, countSel) => {
    const tbody = $(tbodySel);
    tbody.innerHTML = "";
    const countEl = document.querySelector(countSel);
    if (countEl) countEl.textContent = `${list.length} found`;

    if (list.length === 0) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td colspan="5" class="muted">None discovered yet. Click “Scan”.</td>`;
      tbody.appendChild(tr);
      return;
    }

    for (const s of list) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><code>${escapeHtml(s?.host ?? "")}</code></td>
        <td>${escapeHtml(String(s?.port ?? ""))}</td>
        <td><code>${escapeHtml(s?.endpoint ?? "")}</code></td>
        <td>${s?.reachable === true ? "yes" : s?.reachable === false ? "no" : "<span class='muted'>n/a</span>"}</td>
        <td>
          <span class="actions">
          <button class="btn btn-secondary" data-action="test" data-protocol="${escapeHtml(
            s?.protocol ?? ""
          )}" data-host="${escapeHtml(s?.host ?? "")}" data-port="${escapeHtml(String(s?.port ?? ""))}">Test</button>
          <button class="btn btn-primary" data-action="add" data-protocol="${escapeHtml(
            s?.protocol ?? ""
          )}" data-endpoint="${escapeHtml(s?.endpoint ?? "")}" data-host="${escapeHtml(
            s?.host ?? ""
          )}" data-port="${escapeHtml(String(s?.port ?? ""))}">Add as source</button>
          </span>
        </td>
      `;
      tbody.appendChild(tr);
    }
  };

  renderGroup("#discoveryTbodyOpcua", groups.opcua, "#discoveryCountOpcua");
  renderGroup("#discoveryTbodyMqtt", groups.mqtt, "#discoveryCountMqtt");
  renderGroup("#discoveryTbodyModbus", groups.modbus, "#discoveryCountModbus");
}

async function loadZerobus() {
  const cfg = await apiFetch("/api/zerobus/config");
  const z = normalizeZerobusConfigForForm(cfg);

  $("#zbEnabled").checked = z.enabled;
  $("#zbWorkspace").value = z.workspace_host;
  $("#zbZerobusEndpoint").value = z.zerobus_endpoint;
  $("#zbTableFqn").value = z.table_fqn;
  $("#zbClientId").value = z.auth.client_id;
  $("#zbClientSecret").value = "";
  $("#zbSecretHint").textContent = z.auth.has_secret
    ? "A secret is already stored (leave blank to keep it)."
    : "No secret stored yet.";

  // Load proxy settings
  const proxy = z.proxy || {};
  $("#zbProxyEnabled").checked = proxy.enabled || false;
  $("#zbProxyUseEnv").checked = proxy.use_env_vars !== false; // default true
  $("#zbProxyHttp").value = proxy.http_proxy || "";
  $("#zbProxyHttps").value = proxy.https_proxy || "";
  $("#zbProxyNoProxy").value = proxy.no_proxy || "localhost,127.0.0.1";
}

async function saveZerobus() {
  const fqn = $("#zbTableFqn").value.trim();
  const parsed = parseFqn(fqn);
  if (!parsed) {
    toast("Table must be catalog.schema.table", "bad");
    return;
  }

  const payload = {
    enabled: $("#zbEnabled").checked,
    workspace_host: $("#zbWorkspace").value.trim(),
    zerobus_endpoint: $("#zbZerobusEndpoint").value.trim(),
    default_target: {
      workspace_host: $("#zbWorkspace").value.trim(),
      catalog: parsed.catalog,
      schema: parsed.schema,
      table: parsed.table,
    },
    auth: {
      client_id: $("#zbClientId").value.trim(),
    },
    proxy: {
      enabled: $("#zbProxyEnabled").checked,
      use_env_vars: $("#zbProxyUseEnv").checked,
      http_proxy: $("#zbProxyHttp").value.trim(),
      https_proxy: $("#zbProxyHttps").value.trim(),
      no_proxy: $("#zbProxyNoProxy").value.trim(),
    },
  };

  const secret = $("#zbClientSecret").value;
  if (secret && secret.trim()) payload.auth.client_secret = secret.trim();

  const res = await apiFetch("/api/zerobus/config", { method: "POST", body: JSON.stringify(payload) });
  if (res?.warning) toast(res.warning, "warn");
  else toast("ZeroBus config saved", "ok");
  await loadZerobus();
  await refreshStatus();
}


async function runZerobusDiagnostics() {
  const deep = document.getElementById('zbDiagDeep')?.checked ? 'true' : 'false';
  const data = await apiFetch(`/api/zerobus/diagnostics?deep=${deep}`);
  const el = document.getElementById('zbDiag');
  if (el) el.textContent = JSON.stringify(data, null, 2);
  toast(data?.checks?.deep_stream_create_ok === false ? 'ZeroBus diagnostics: issues found' : 'ZeroBus diagnostics: ok', data?.checks?.deep_stream_create_ok === false ? 'warn' : 'ok');
}


async function startBridge() {
  const res = await apiFetch("/api/bridge/start", { method: "POST", body: "{}" });
  toast(res?.message || "Bridge start requested", "ok");
  // Give it a moment to connect sources, then refresh
  setTimeout(() => refreshStatus().catch(() => {}), 800);
}

async function stopBridge() {
  const res = await apiFetch("/api/bridge/stop", { method: "POST", body: "{}" });
  toast(res?.message || "Bridge stop requested", "warn");
  await refreshStatus();
}

async function startZerobus() {
  const res = await apiFetch("/api/zerobus/start", { method: "POST", body: "{}" });
  toast(res?.message || "ZeroBus start requested", "ok");
  await refreshStatus();
}

async function stopZerobus() {
  const res = await apiFetch("/api/zerobus/stop", { method: "POST", body: "{}" });
  toast(res?.message || "ZeroBus stop requested", "warn");
  await refreshStatus();
}

function fillSourceForm(source) {
  $("#srcName").value = source?.name ?? "";
  $("#srcProtocol").value = source?.protocol ?? "opcua";
  $("#srcEndpoint").value = source?.endpoint ?? "";
  $("#srcEnabled").checked = Boolean(source?.enabled ?? true);

  $("#srcSite").value = source?.site ?? "";
  $("#srcArea").value = source?.area ?? "";
  $("#srcLine").value = source?.line ?? "";
  $("#srcEquipment").value = source?.equipment ?? "";

  $("#srcMode").value = "edit";
  $("#srcOriginalName").value = source?.name ?? "";
  $("#srcSubmit").textContent = "Save source";
}

function resetSourceForm() {
  $("#srcName").value = "";
  $("#srcProtocol").value = "opcua";
  $("#srcEndpoint").value = "";
  $("#srcEnabled").checked = true;
  $("#srcSite").value = "";
  $("#srcArea").value = "";
  $("#srcLine").value = "";
  $("#srcEquipment").value = "";
  $("#srcMode").value = "create";
  $("#srcOriginalName").value = "";
  $("#srcSubmit").textContent = "Add source";
}

async function submitSourceForm(e) {
  e.preventDefault();

  const payload = {
    name: $("#srcName").value.trim(),
    protocol: $("#srcProtocol").value,
    endpoint: $("#srcEndpoint").value.trim(),
    enabled: $("#srcEnabled").checked,
  };

  for (const [id, key] of [
    ["#srcSite", "site"],
    ["#srcArea", "area"],
    ["#srcLine", "line"],
    ["#srcEquipment", "equipment"],
  ]) {
    const v = $(id).value.trim();
    if (v) payload[key] = v;
  }

  const mode = $("#srcMode").value;
  if (mode === "edit") {
    const originalName = $("#srcOriginalName").value;
    await apiFetch(`/api/sources/${encodeURIComponent(originalName)}`, {
      method: "PUT",
      body: JSON.stringify(payload),
    });
    toast(`Source updated: ${payload.name}`, "ok");
  } else {
    await apiFetch("/api/sources", { method: "POST", body: JSON.stringify(payload) });
    toast(`Source added: ${payload.name}`, "ok");
  }

  resetSourceForm();
  await refreshSources();
  await refreshStatus();
}



function getSelectedSourceNames() {
  return Array.from(document.querySelectorAll('input[data-select="source"]:checked'))
    .map((el) => el.getAttribute('data-name'))
    .filter(Boolean);
}

async function startSelectedSources() {
  const names = getSelectedSourceNames();
  if (names.length === 0) {
    toast('Select at least one source', 'warn');
    return;
  }
  for (const n of names) {
    await apiFetch(`/api/sources/${encodeURIComponent(n)}/start`, { method: 'POST', body: '{}' });
  }
  toast(`Started ${names.length} source(s)`, 'ok');
  await refreshSources();
  await refreshStatus();
}

async function stopSelectedSources() {
  const names = getSelectedSourceNames();
  if (names.length === 0) {
    toast('Select at least one source', 'warn');
    return;
  }
  for (const n of names) {
    await apiFetch(`/api/sources/${encodeURIComponent(n)}/stop`, { method: 'POST', body: '{}' });
  }
  toast(`Stopped ${names.length} source(s)`, 'warn');
  await refreshSources();
  await refreshStatus();
}

async function startSource(name) {
  const res = await apiFetch(`/api/sources/${encodeURIComponent(name)}/start`, { method: "POST", body: "{}" });
  toast(res?.message || `Started: ${name}`, "ok");
  await refreshSources();
  await refreshStatus();
}

async function stopSource(name) {
  const res = await apiFetch(`/api/sources/${encodeURIComponent(name)}/stop`, { method: "POST", body: "{}" });
  toast(res?.message || `Stopped: ${name}`, "warn");
  await refreshSources();
  await refreshStatus();
}

async function deleteSource(name) {
  if (!confirm(`Delete source "${name}"?`)) return;
  await apiFetch(`/api/sources/${encodeURIComponent(name)}`, { method: "DELETE" });
  toast(`Deleted source: ${name}`, "warn");
  await refreshSources();
  await refreshStatus();
}

async function editSource(name) {
  const data = await apiFetch("/api/sources");
  const sources = Array.isArray(data?.sources) ? data.sources : [];
  const s = sources.find((x) => x?.name === name);
  if (!s) throw new Error(`Source not found: ${name}`);
  fillSourceForm(s);
  toast(`Editing source: ${name}`, "ok");
}


function suggestSourceName(protocol, host, port) {
  const base = `${protocol || 'src'}-${host || 'host'}-${port || '0'}`;
  return base.replaceAll(/[^a-zA-Z0-9_-]/g, "-").toLowerCase();
}

function prefillCreateSourceFromDiscovery(protocol, endpoint, host, port) {
  resetSourceForm();
  $("#srcName").value = suggestSourceName(protocol, host, port);
  $("#srcProtocol").value = protocol || "opcua";
  $("#srcEndpoint").value = endpoint || "";
  $("#srcEnabled").checked = true;
  toast(`Prefilled source from discovery: ${protocol} ${host}:${port}`, "ok");
  // Scroll into view
  document.getElementById('sourceForm')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

async function scanDiscovery() {
  await apiFetch("/api/discovery/scan", { method: "POST", body: "{}" });
  toast("Discovery scan started", "ok");
  setTimeout(() => refreshDiscovered().catch(() => {}), 1200);
}

async function testDiscoveredServer(protocol, host, port) {
  const res = await apiFetch("/api/discovery/test", {
    method: "POST",
    body: JSON.stringify({ protocol, host, port: Number(port) }),
  });
  const out = document.getElementById("discoveryTestResult");
  if (out) out.textContent = JSON.stringify(res, null, 2);
  toast(res?.ok === true ? "Discovery test: ok" : "Discovery test: failed", res?.ok === true ? "ok" : "warn");
}

function onError(err) {
  console.error(err);
  toast(err?.message || String(err), "bad");
}

function wireEvents() {
  $("#btnRefresh").addEventListener("click", () => refreshAll().catch(onError));
  $("#btnRefreshStatus").addEventListener("click", () => refreshStatus().catch(onError));
  const srcRefresh = document.getElementById("btnRefreshSources");
  if (srcRefresh) srcRefresh.addEventListener("click", () => refreshSources().catch(onError));
  $("#btnRefreshMetrics").addEventListener("click", () => refreshMetrics().catch(onError));
  const pipelineRefresh = document.getElementById("btnRefreshPipeline");
  if (pipelineRefresh) pipelineRefresh.addEventListener("click", () => refreshPipeline().catch(onError));

  const selectAll = document.getElementById('srcSelectAll');
  if (selectAll) {
    selectAll.addEventListener('change', () => {
      const checked = selectAll.checked;
      for (const cb of document.querySelectorAll('input[data-select="source"]')) {
        cb.checked = checked;
      }
    });
  }

  const startBridgeBtn = document.getElementById('btnStartBridge');
  if (startBridgeBtn) startBridgeBtn.addEventListener('click', () => startBridge().catch(onError));
  const startSel = document.getElementById('btnStartSelected');
  if (startSel) startSel.addEventListener('click', () => startSelectedSources().catch(onError));
  const stopSel = document.getElementById('btnStopSelected');
  if (stopSel) stopSel.addEventListener('click', () => stopSelectedSources().catch(onError));

  $("#btnDiscoveryScan").addEventListener("click", () => scanDiscovery().catch(onError));

  const onDiscoveryClick = (e) => {
    const btn = e.target.closest("button[data-action]");
    if (!btn) return;
    if (btn.getAttribute("data-action") === "test") {
      testDiscoveredServer(
        btn.getAttribute("data-protocol"),
        btn.getAttribute("data-host"),
        btn.getAttribute("data-port")
      ).catch(onError);
    }
    if (btn.getAttribute("data-action") === "add") {
      prefillCreateSourceFromDiscovery(
        btn.getAttribute("data-protocol"),
        btn.getAttribute("data-endpoint"),
        btn.getAttribute("data-host"),
        btn.getAttribute("data-port")
      );
    }
  };
  // Three protocol tbodies
  document.getElementById("discoveryTbodyOpcua")?.addEventListener("click", onDiscoveryClick);
  document.getElementById("discoveryTbodyMqtt")?.addEventListener("click", onDiscoveryClick);
  document.getElementById("discoveryTbodyModbus")?.addEventListener("click", onDiscoveryClick);

  $("#sourcesTbody").addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-action]");
    if (!btn) return;
    const action = btn.getAttribute("data-action");
    const name = btn.getAttribute("data-name");
    if (action === "start") startSource(name).catch(onError);
    if (action === "stop") stopSource(name).catch(onError);
    if (action === "delete") deleteSource(name).catch(onError);
    if (action === "edit") editSource(name).catch(onError);
  });

  $("#sourceForm").addEventListener("submit", (e) => submitSourceForm(e).catch(onError));
  $("#srcReset").addEventListener("click", (e) => {
    e.preventDefault();
    resetSourceForm();
  });

  $("#btnLoadZerobus").addEventListener("click", () => loadZerobus().catch(onError));
  $("#btnSaveZerobus").addEventListener("click", () => saveZerobus().catch(onError));
  $("#btnStartZerobus").addEventListener("click", () => startZerobus().catch(onError));
  $("#btnStopZerobus").addEventListener("click", () => stopZerobus().catch(onError));

  const diagBtn = document.getElementById('btnZerobusDiag');
  if (diagBtn) diagBtn.addEventListener('click', () => runZerobusDiagnostics().catch(onError));

  // Collapsible cards (ZeroBus / Status / Metrics)
  for (const btn of document.querySelectorAll('button[data-toggle="collapse"]')) {
    btn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      const target = btn.getAttribute("data-target");
      if (!target) return;
      const body = document.querySelector(target);
      if (!body) return;
      const isOpen = body.style.display !== "none";
      body.style.display = isOpen ? "none" : "block";
      btn.textContent = isOpen ? "▾" : "▴";
    });
  }

  // Allow clicking the header to expand/collapse (except when clicking buttons/inputs)
  for (const header of document.querySelectorAll(".card.collapsible .card-header")) {
    header.addEventListener("click", (e) => {
      const t = e.target;
      if (t && (t.closest("button") || t.closest("input") || t.closest("select") || t.closest("a"))) return;
      const btn = header.querySelector('button[data-toggle="collapse"][data-target]');
      if (btn) btn.click();
    });
  }
  for (const header of document.querySelectorAll(".subcard .subcard-header")) {
    header.addEventListener("click", (e) => {
      const t = e.target;
      if (t && (t.closest("button") || t.closest("input") || t.closest("select") || t.closest("a"))) return;
      const btn = header.querySelector('button[data-toggle="collapse"][data-target]');
      if (btn) btn.click();
    });
  }
}

async function refreshAll() {
  await Promise.allSettled([
    refreshStatus(),
    refreshMetrics(),
    refreshSources(),
    refreshDiscovered(),
  ]);
}

// Authentication functions

async function checkAuth() {
  try {
    const authStatus = await apiFetch("/api/auth/status");

    // If authentication is disabled, skip auth check
    if (!authStatus.auth_enabled) {
      console.log("Authentication disabled - bypassing auth check");
      return true;
    }

    // If authentication is enabled, check if user is authenticated
    if (!authStatus.authenticated) {
      window.location.href = "/static/login.html";
      return false;
    }
    currentUser = authStatus.user;
    return true;
  } catch (error) {
    console.error("Auth check failed:", error);
    window.location.href = "/static/login.html";
    return false;
  }
}

async function loadUserPermissions() {
  try {
    const authStatus = await apiFetch("/api/auth/status");

    // If authentication is disabled, grant all permissions
    if (!authStatus.auth_enabled) {
      userPermissions = ["read", "write", "configure", "start_stop", "delete", "manage_users"];
      console.log("Authentication disabled - granting all permissions");
      return true;
    }

    // If authentication is enabled, load user permissions
    const permData = await apiFetch("/api/auth/permissions");
    userPermissions = permData.permissions || [];
    return true;
  } catch (error) {
    console.error("Failed to load permissions:", error);
    return false;
  }
}

function hasPermission(permission) {
  return userPermissions.includes(permission);
}

function canRead() {
  return hasPermission("read");
}

function canWrite() {
  return hasPermission("write");
}

function canConfigure() {
  return hasPermission("configure");
}

function canStartStop() {
  return hasPermission("start_stop");
}

function canDelete() {
  return hasPermission("delete");
}

function adaptUIForPermissions() {
  // Disable/hide buttons based on permissions

  // Discovery scan requires WRITE
  const btnDiscoveryScan = document.getElementById("btnDiscoveryScan");
  if (btnDiscoveryScan && !canWrite()) {
    btnDiscoveryScan.disabled = true;
    btnDiscoveryScan.title = "Requires write permission";
  }

  // Source add/edit requires WRITE
  const srcSubmit = document.getElementById("srcSubmit");
  if (srcSubmit && !canWrite()) {
    srcSubmit.disabled = true;
    srcSubmit.title = "Requires write permission";
  }

  // Start/stop buttons require START_STOP
  for (const btn of document.querySelectorAll('[data-action="start"], [data-action="stop"]')) {
    if (!canStartStop()) {
      btn.disabled = true;
      btn.title = "Requires start/stop permission";
    }
  }

  const btnStartBridge = document.getElementById("btnStartBridge");
  if (btnStartBridge && !canStartStop()) {
    btnStartBridge.disabled = true;
    btnStartBridge.title = "Requires start/stop permission";
  }

  const btnStartSelected = document.getElementById("btnStartSelected");
  if (btnStartSelected && !canStartStop()) {
    btnStartSelected.disabled = true;
    btnStartSelected.title = "Requires start/stop permission";
  }

  const btnStopSelected = document.getElementById("btnStopSelected");
  if (btnStopSelected && !canStartStop()) {
    btnStopSelected.disabled = true;
    btnStopSelected.title = "Requires start/stop permission";
  }

  const btnStartZerobus = document.getElementById("btnStartZerobus");
  if (btnStartZerobus && !canStartStop()) {
    btnStartZerobus.disabled = true;
    btnStartZerobus.title = "Requires start/stop permission";
  }

  const btnStopZerobus = document.getElementById("btnStopZerobus");
  if (btnStopZerobus && !canStartStop()) {
    btnStopZerobus.disabled = true;
    btnStopZerobus.title = "Requires start/stop permission";
  }

  // ZeroBus config save requires CONFIGURE
  const btnSaveZerobus = document.getElementById("btnSaveZerobus");
  if (btnSaveZerobus && !canConfigure()) {
    btnSaveZerobus.disabled = true;
    btnSaveZerobus.title = "Requires configure permission (admin only)";
  }

  // Delete buttons require DELETE
  for (const btn of document.querySelectorAll('[data-action="delete"]')) {
    if (!canDelete()) {
      btn.disabled = true;
      btn.title = "Requires delete permission (admin only)";
    }
  }

  // If viewer role (read only), add visual indicator
  if (canRead() && !canWrite()) {
    const title = document.querySelector(".topbar .title h1");
    if (title) {
      const badge = document.createElement("span");
      badge.style.cssText = "font-size: 14px; color: #666; margin-left: 12px; font-weight: normal;";
      badge.textContent = "(Read-Only)";
      title.appendChild(badge);
    }
  }
}

function displayUserInfo() {
  if (!currentUser) return;

  // Add user info to topbar
  const topbar = document.querySelector(".topbar .title");
  if (!topbar) return;

  const userInfo = document.createElement("div");
  userInfo.style.cssText = "display: flex; align-items: center; gap: 12px; font-size: 14px;";
  userInfo.innerHTML = `
    <span style="color: #666;">
      <strong>${escapeHtml(currentUser.name || currentUser.email)}</strong>
      <span style="margin-left: 8px; color: #999;">(${escapeHtml(currentUser.role)})</span>
    </span>
    <button id="btnLogout" class="btn btn-secondary" style="padding: 6px 12px; font-size: 13px;">Logout</button>
  `;

  topbar.appendChild(userInfo);

  // Wire logout button
  const btnLogout = document.getElementById("btnLogout");
  if (btnLogout) {
    btnLogout.addEventListener("click", async () => {
      try {
        await fetch("/logout", {
          method: "POST",
          credentials: "include"
        });
        window.location.href = "/static/login.html";
      } catch (error) {
        console.error("Logout failed:", error);
        toast("Logout failed", "bad");
      }
    });
  }
}

async function boot() {
  try {
    // Check authentication first
    const authenticated = await checkAuth();
    if (!authenticated) return;

    // Load user permissions
    await loadUserPermissions();

    // Display user info in header
    displayUserInfo();

    // Wire up event handlers
    wireEvents();
    resetSourceForm();

    // Load data
    await loadZerobus();
    await refreshAll();

    // Adapt UI based on permissions (must be after wireEvents and refreshAll)
    adaptUIForPermissions();

    // Auto-refresh pipeline diagnostics every 3 seconds
    setInterval(() => {
      const pipelineBody = document.getElementById("pipelineBody");
      if (pipelineBody && pipelineBody.style.display !== "none") {
        refreshPipeline().catch(e => console.error("Pipeline auto-refresh failed:", e));
      }
    }, 3000);

    toast("UI ready", "ok");
  } catch (e) {
    onError(e);
  }
}

window.addEventListener("DOMContentLoaded", boot);
