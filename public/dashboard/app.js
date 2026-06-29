// State Management
const simState = {
  isRunning: true,
  speedScale: 1.0,
  isWarningActive: false,
  selectedNode: null, // null means overall pipeline
  tickInterval: null,
  historyLength: 20
};

// Canvas Pan & Zoom State
const canvasState = {
  panX: 0,
  panY: 0,
  scale: 1.0
};

// Pipeline Nodes Configuration
const nodesConfig = {
  'operazioni-bancarie': {
    name: 'Operazioni Bancarie',
    type: 'Source',
    description: 'Main banking transactions ingestion endpoint. Reads streams from message queues, parses ISO-8583 banking protocols, and emits normalized transaction events.',
    baseMetrics: { throughput: 6.2, latency: 0.1, cpu: 12, mem: 3.2 },
    customInfo: (metrics) => `<strong>Active Ingestors:</strong> 4 workers<br><strong>Buffer Queue:</strong> 0.2% full`,
    inputSample: `{\n  "raw_payload": "NjgxMiwxNTAuMDAsMTcxOTMyNDgwMCx1c3JfNDIwMQ==",\n  "channel": "ATM-Gateway-04",\n  "protocol": "ISO-8583"\n}`,
    outputSample: `{\n  "tx_id": "tx_2049182",\n  "user_id": "usr_4201",\n  "amount": 150.00,\n  "timestamp": "2026-06-26T10:45:00Z"\n}`
  },
  'controlli-utente': {
    name: 'Controlli Utente',
    type: 'Operator',
    description: 'Performs verification of user credentials, KYC checks, and risk thresholds.',
    baseMetrics: { throughput: 5.1, latency: 1.8, cpu: 20, mem: 5.0 },
    customInfo: (metrics) => {
      if (simState.isWarningActive) {
        return `<span style="color:#e03131; font-weight:bold;">⚠️ Alert: Rate Limit exceeded!</span><br><strong>Failure rate:</strong> 4.2%<br><strong>Warning:</strong> Response times spiking.`;
      }
      return `<strong>Active Checks:</strong> 12 policies<br><strong>Cache Hit Rate:</strong> 94.2%`;
    },
    inputSample: `{\n  "tx_id": "tx_2049182",\n  "user_id": "usr_4201",\n  "amount": 150.00,\n  "timestamp": "2026-06-26T10:45:00Z"\n}`,
    outputSample: () => {
      if (simState.isWarningActive) {
        return JSON.stringify({
          "tx_id": "tx_2049182",
          "user_id": "usr_4201",
          "amount": 150.00,
          "timestamp": "2026-06-26T10:45:00Z",
          "user_verified": true,
          "risk_score": 0.94,
          "warnings": ["RATE_LIMIT_EXCEEDED"]
        }, null, 2);
      }
      return JSON.stringify({
        "tx_id": "tx_2049182",
        "user_id": "usr_4201",
        "amount": 150.00,
        "timestamp": "2026-06-26T10:45:00Z",
        "user_verified": true,
        "risk_score": 0.12
      }, null, 2);
    }
  },
  'controlli-transazione': {
    name: 'Controlli Transazione',
    type: 'Operator',
    description: 'Evaluates transaction payloads for compliance thresholds, limit checks, and historical daily limits.',
    baseMetrics: { throughput: 4.9, latency: 2.2, cpu: 18, mem: 4.8 },
    customInfo: (metrics) => `<strong>Rule Engine:</strong> Drools v8.1<br><strong>Evaluation Latency:</strong> 1.5ms`,
    inputSample: `{\n  "tx_id": "tx_2049182",\n  "user_id": "usr_4201",\n  "amount": 150.00,\n  "timestamp": "2026-06-26T10:45:00Z"\n}`,
    outputSample: `{\n  "tx_id": "tx_2049182",\n  "user_id": "usr_4201",\n  "amount": 150.00,\n  "timestamp": "2026-06-26T10:45:00Z",\n  "compliance_status": "PASSED",\n  "limit_check": "OK"\n}`
  },
  'salvataggio-audit-pre': {
    name: 'Salvataggio Audit',
    type: 'Sink',
    description: 'Persists raw pre-filtered transaction payloads directly into cold storage audit logs for security history.',
    baseMetrics: { throughput: 1.1, latency: 12.5, cpu: 15, mem: 2.1 },
    customInfo: (metrics) => `<strong>Storage Target:</strong> S3 Glacier Backup<br><strong>Write Status:</strong> Acknowledged`,
    inputSample: `{\n  "tx_id": "tx_2049182",\n  "user_id": "usr_4201",\n  "amount": 150.00,\n  "timestamp": "2026-06-26T10:45:00Z"\n}`,
    outputSample: `{\n  "db_status": "SUCCESS",\n  "inserted_rows": 1,\n  "table": "raw_audit_logs",\n  "hash": "8f3b23c21a4f00129bc90812"\n}`
  },
  'accetta-rifiuto': {
    name: 'Accetta / Rifiuto',
    type: 'Filter',
    description: 'Evaluates transaction payload risk metrics and verification status to filter or route payloads.',
    baseMetrics: { throughput: 4.8, latency: 0.5, cpu: 22, mem: 5.1 },
    customInfo: (metrics) => {
      const acceptRate = simState.isWarningActive ? '88%' : '98%';
      const rejectRate = simState.isWarningActive ? '12%' : '2%';
      return `<strong>Filter Rates:</strong><br><span style="color:#2b8a3e; font-weight:bold;">✔ Accept: ${acceptRate}</span><br><span style="color:#e03131; font-weight:bold;">✖ Reject: ${rejectRate}</span>`;
    },
    inputSample: `{\n  "tx_id": "tx_2049182",\n  "user_verified": true,\n  "risk_score": 0.12,\n  "compliance_status": "PASSED"\n}`,
    outputSample: () => {
      if (simState.isWarningActive) {
        return JSON.stringify({
          "tx_id": "tx_2049182",
          "status": "REJECTED",
          "reason": "RISK_SCORE_THRESHOLD_EXCEEDED"
        }, null, 2);
      }
      return JSON.stringify({
        "tx_id": "tx_2049182",
        "status": "APPROVED",
        "routing_target": "POST_PROCESSING"
      }, null, 2);
    }
  },
  'invio-conferma': {
    name: 'Invio Conferma',
    type: 'Sink',
    description: 'Dispatches notifications to users via SMS or Email notifications for successful transactions.',
    baseMetrics: { throughput: 4.7, latency: 15.2, cpu: 10, mem: 1.8 },
    customInfo: (metrics) => `<strong>SMTP Gateway:</strong> Connected<br><strong>Queue Size:</strong> 0 messages pending`,
    inputSample: `{\n  "tx_id": "tx_2049182",\n  "status": "APPROVED"\n}`,
    outputSample: `{\n  "notification_sent": true,\n  "channel": "SMS",\n  "to": "+39 333 420104",\n  "status": "DELIVERED"\n}`
  },
  'salvataggio-audit-post': {
    name: 'Salvataggio Audit',
    type: 'Sink',
    description: 'Saves filtered and approved ledger items into PostgreSQL db tables for downstream accounting.',
    baseMetrics: { throughput: 4.7, latency: 8.1, cpu: 11, mem: 2.3 },
    customInfo: (metrics) => `<strong>DB Target:</strong> PostgreSQL Audit Cluster<br><strong>Pool Connections:</strong> 12/20 active`,
    inputSample: `{\n  "tx_id": "tx_2049182",\n  "status": "APPROVED"\n}`,
    outputSample: `{\n  "db_status": "SUCCESS",\n  "inserted_rows": 1,\n  "table": "processed_ledger",\n  "ledger_id": "L-908124"\n}`
  }
};

// Global metrics templates
const globalPipelineConfig = {
  name: 'Pipeline Statistics',
  type: 'Global',
  description: 'Global monitoring parameters for the entire Renoir banking pipeline.',
  baseMetrics: { throughput: 3.1, latency: 2.0, cpu: 20, mem: 5.0 },
  customInfo: (metrics) => `<strong>Active Operators:</strong> 7 nodes<br><strong>Engine Threads:</strong> 16<br><strong>Pipeline Status:</strong> ${simState.isWarningActive ? 'WARNING' : 'HEALTHY'}`
};

// Active runtime metrics values
const metricsData = {};

// History arrays for sparkline charts
const historyData = {
  global: []
};

// Initialize metrics and histories
function initMetrics() {
  // Global
  metricsData['global'] = { ...globalPipelineConfig.baseMetrics };
  historyData['global'] = Array.from({ length: simState.historyLength }, () => globalPipelineConfig.baseMetrics.throughput);

  // Nodes
  Object.keys(nodesConfig).forEach(id => {
    metricsData[id] = { ...nodesConfig[id].baseMetrics };
    historyData[id] = Array.from({ length: simState.historyLength }, () => nodesConfig[id].baseMetrics.throughput);
  });
}

// Generate slight metric fluctuations (random walk)
function updateMetrics() {
  if (!simState.isRunning) return;

  const warnFactor = simState.isWarningActive ? 1.5 : 1.0;

  // 1. Update nodes
  Object.keys(nodesConfig).forEach(id => {
    const config = nodesConfig[id];
    const metrics = metricsData[id];

    // Base fluctuation
    let devT = (Math.random() - 0.5) * 0.1;
    let devL = (Math.random() - 0.5) * 0.3;
    let devC = (Math.random() - 0.5) * 1.0;
    
    // Apply simulation scale
    metrics.throughput = Math.max(0.1, config.baseMetrics.throughput * simState.speedScale + devT);

    if (simState.isWarningActive && (id === 'controlli-utente' || id === 'controlli-transazione')) {
      // Spikes in CPU and Latency, drop in throughput
      metrics.throughput *= 0.7;
      metrics.latency = config.baseMetrics.latency * 2.5 + (Math.random() * 2.0);
      metrics.cpu = Math.min(99, config.baseMetrics.cpu * 3.8 + (Math.random() * 5));
    } else {
      metrics.latency = Math.max(0.05, config.baseMetrics.latency + devL);
      metrics.cpu = Math.max(2, Math.min(98, config.baseMetrics.cpu + devC));
    }
    
    metrics.mem = Math.max(0.5, config.baseMetrics.mem + (Math.random() - 0.5) * 0.05);

    // Save history
    historyData[id].push(metrics.throughput);
    if (historyData[id].length > simState.historyLength) historyData[id].shift();
  });

  // 2. Update global metrics
  const globalMetrics = metricsData['global'];
  let devT = (Math.random() - 0.5) * 0.05;
  let devL = (Math.random() - 0.5) * 0.1;
  let devC = (Math.random() - 0.5) * 0.5;

  globalMetrics.throughput = Math.max(0.1, globalPipelineConfig.baseMetrics.throughput * simState.speedScale + devT);
  
  if (simState.isWarningActive) {
    globalMetrics.latency = globalPipelineConfig.baseMetrics.latency * 2.0 + devL;
    globalMetrics.cpu = Math.min(95, globalPipelineConfig.baseMetrics.cpu * 2.2 + devC);
    // Add event warning occasionally
    if (Math.random() > 0.6) {
      logToConsole('WARNING: Controlli Utente: Processing rate limit exceeded. Retrying payloads.', 'warn');
    }
  } else {
    globalMetrics.latency = Math.max(0.1, globalPipelineConfig.baseMetrics.latency + devL);
    globalMetrics.cpu = Math.max(2, Math.min(98, globalPipelineConfig.baseMetrics.cpu + devC));
  }
  globalMetrics.mem = Math.max(1, globalPipelineConfig.baseMetrics.mem + (Math.random() - 0.5) * 0.02);

  // Save global history
  historyData['global'].push(globalMetrics.throughput);
  if (historyData['global'].length > simState.historyLength) historyData['global'].shift();

  // 3. Update active UI elements
  updateInspectorPanel();
  updateFloatingBubbles();
}

// Draw the connection curves using SVG
function drawConnections() {
  const svg = document.getElementById('connections-svg');
  const viewport = document.getElementById('dag-viewport');
  if (!svg || !viewport) return;

  const viewportRect = viewport.getBoundingClientRect();
  const scale = canvasState.scale;
  
  // Clear connections and paths, keeping defs if any (we draw on top)
  // To avoid clearing markers, remove only paths and particles
  const paths = svg.querySelectorAll('.connection-path, .particle-group');
  paths.forEach(p => p.remove());

  // Define DAG links
  const links = [
    { from: 'operazioni-bancarie', to: 'controlli-utente' },
    { from: 'operazioni-bancarie', to: 'controlli-transazione' },
    { from: 'operazioni-bancarie', to: 'salvataggio-audit-pre' },
    { from: 'controlli-utente', to: 'accetta-rifiuto' },
    { from: 'controlli-transazione', to: 'accetta-rifiuto' },
    { from: 'accetta-rifiuto', to: 'invio-conferma' },
    { from: 'accetta-rifiuto', to: 'salvataggio-audit-post' }
  ];

  links.forEach(link => {
    const fromEl = document.getElementById(`node-${link.from}`);
    const toEl = document.getElementById(`node-${link.to}`);
    if (!fromEl || !toEl) return;

    const fromRect = fromEl.getBoundingClientRect();
    const toRect = toEl.getBoundingClientRect();

    // From node center-right (normalized to scale-independent viewport coordinates)
    const x1 = (fromRect.right - viewportRect.left) / scale;
    const y1 = (fromRect.top + fromRect.height / 2 - viewportRect.top) / scale;

    // To node center-left (normalized and offset for arrowhead marker size)
    const x2 = (toRect.left - viewportRect.left) / scale - 4;
    const y2 = (toRect.top + toRect.height / 2 - viewportRect.top) / scale;

    // Control point offset
    const dx = Math.abs(x2 - x1) * 0.45;
    
    // Create connection path d string
    const d = `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`;

    // Create Path
    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    path.setAttribute('d', d);
    
    // Highlight active selected node paths
    let isActive = false;
    if (simState.selectedNode) {
      if (simState.selectedNode === link.from || simState.selectedNode === link.to) {
        isActive = true;
      }
    }
    
    path.setAttribute('class', `connection-path ${isActive ? 'active' : ''}`);
    
    // Select marker-end arrow style based on active theme & state
    let markerId = 'arrow-black';
    const isDark = document.body.classList.contains('theme-neobrutalist-dark');
    
    if (isActive) {
      markerId = 'arrow-green';
    } else if (isDark) {
      markerId = 'arrow-white';
    } else {
      markerId = 'arrow-black';
    }
    path.setAttribute('marker-end', `url(#${markerId})`);

    svg.appendChild(path);

    // Spawn animated floating bubbles (particles) along the path
    if (simState.isRunning) {
      // Speed adjustments
      const duration = (2.2 / simState.speedScale).toFixed(1) + 's';
      
      // We spawn 2 particles along each path, staggered
      for (let i = 0; i < 2; i++) {
        const delay = ((i * 1.1) / simState.speedScale).toFixed(1) + 's';

        const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        g.setAttribute('class', 'particle-group');

        // Main bubble circle
        const bubble = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        bubble.setAttribute('r', '5.5');
        bubble.setAttribute('class', 'particle-bubble');
        if (isActive) {
          bubble.style.stroke = '#2b8a3e';
          bubble.style.fill = '#ebfbee';
        }
        g.appendChild(bubble);

        // Shiny dot for cartoon shading
        const glint = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        glint.setAttribute('cx', '-1.5');
        glint.setAttribute('cy', '-1.5');
        glint.setAttribute('r', '1.2');
        glint.setAttribute('class', 'particle-glint');
        g.appendChild(glint);

        // Motion animation along path
        const animate = document.createElementNS('http://www.w3.org/2000/svg', 'animateMotion');
        animate.setAttribute('path', d);
        animate.setAttribute('dur', duration);
        animate.setAttribute('begin', delay);
        animate.setAttribute('repeatCount', 'indefinite');
        g.appendChild(animate);

        svg.appendChild(g);
      }
    }
  });
}

// Draw custom sketchy SVG sparkline chart
function drawSparkline(values) {
  const pathEl = document.getElementById('sparkline-path');
  if (!pathEl || !values || values.length === 0) return;

  const width = 300;
  const height = 80;
  const padding = 6;

  const min = Math.min(...values) * 0.95;
  const max = Math.max(...values) * 1.05;
  const range = max - min || 1;

  const stepX = (width - padding * 2) / (values.length - 1);
  
  let pathD = '';

  for (let i = 0; i < values.length; i++) {
    const val = values[i];
    const x = padding + i * stepX;
    // Map value to Y coordinate (0 is top, height is bottom)
    const y = height - padding - ((val - min) / range) * (height - padding * 2);

    // Apply sketchy handdrawn wiggle (wiggle values slightly on X/Y to look handwritten)
    const wiggleX = (Math.random() - 0.5) * 0.8;
    const wiggleY = (Math.random() - 0.5) * 0.8;

    if (i === 0) {
      pathD += `M ${x + wiggleX} ${y + wiggleY}`;
    } else {
      // Use cubic curves for smooth sketchy connections
      const prevVal = values[i - 1];
      const prevX = padding + (i - 1) * stepX;
      const prevY = height - padding - ((prevVal - min) / range) * (height - padding * 2);
      
      const cpX1 = prevX + stepX * 0.5 + (Math.random() - 0.5) * 1.5;
      const cpY1 = prevY + (Math.random() - 0.5) * 1.5;
      const cpX2 = prevX + stepX * 0.5 + (Math.random() - 0.5) * 1.5;
      const cpY2 = y + (Math.random() - 0.5) * 1.5;

      pathD += ` C ${cpX1} ${cpY1}, ${cpX2} ${cpY2}, ${x + wiggleX} ${y + wiggleY}`;
    }
  }

  pathEl.setAttribute('d', pathD);
  
  // Set chart stroke color based on status
  if (simState.selectedNode && simState.selectedNode === 'controlli-utente' && simState.isWarningActive) {
    pathEl.setAttribute('stroke', '#e03131'); // red chart
  } else {
    pathEl.setAttribute('stroke', '#2b8a3e'); // green chart
  }
}

// Update floating bubbles stats shown inside the DAG canvas for all nodes
function updateFloatingBubbles() {
  Object.keys(nodesConfig).forEach(id => {
    const metrics = metricsData[id];
    if (!metrics) return;

    const tEl = document.getElementById(`bubble-stat-${id}-throughput`);
    const cEl = document.getElementById(`bubble-stat-${id}-cpu`);
    const mEl = document.getElementById(`bubble-stat-${id}-mem`);

    if (tEl) tEl.textContent = `${metrics.throughput.toFixed(1)}M/s`;
    if (cEl) cEl.textContent = `${Math.round(metrics.cpu)}%`;
    if (mEl) mEl.textContent = `${metrics.mem.toFixed(1)}Gb`;

    // Handle node-specific custom fields inside bubbles
    const customEl = document.getElementById(`bubble-stat-${id}-custom`);
    if (customEl) {
      if (id === 'accetta-rifiuto') {
        const acceptPct = simState.isWarningActive ? 88 : 98;
        const rejectPct = simState.isWarningActive ? 12 : 2;
        customEl.innerHTML = `% Filtrati: <span class="bubble-stat">${acceptPct}% Accetta, ${rejectPct}% Rifiuto</span>`;
      } else if (id === 'controlli-utente') {
        if (simState.isWarningActive) {
          customEl.innerHTML = `<div class="warning-alert-text">Eventi: Warning Rate Limit!</div>`;
        } else {
          customEl.innerHTML = '';
        }
      }
    }
  });
}

// Update the right-hand notebook details panel
function updateInspectorPanel() {
  const titleEl = document.getElementById('inspector-title');
  const badgeEl = document.getElementById('inspector-badge');
  const statusEl = document.getElementById('stat-status');
  const throughputEl = document.getElementById('stat-throughput');
  const latencyEl = document.getElementById('stat-latency');
  const cpuEl = document.getElementById('stat-cpu');
  const memEl = document.getElementById('stat-mem');
  const customSectionEl = document.getElementById('inspector-custom-section');
  const customTextEl = document.getElementById('inspector-custom-text');

  const selected = simState.selectedNode;

  if (selected && nodesConfig[selected]) {
    // Show selected node details
    const config = nodesConfig[selected];
    const metrics = metricsData[selected];

    titleEl.textContent = config.name;
    badgeEl.textContent = config.type;
    badgeEl.className = 'notebook-badge node-badge';

    // Status mapping
    if (selected === 'controlli-utente' && simState.isWarningActive) {
      statusEl.innerHTML = '<span style="color:#e03131; font-weight:bold;">⚠️ Warning</span>';
      badgeEl.className = 'notebook-badge warning-badge';
    } else {
      statusEl.innerHTML = '<span style="color:#2b8a3e; font-weight:bold;">✔ Healthy</span>';
    }

    throughputEl.textContent = `${metrics.throughput.toFixed(2)} M/s`;
    latencyEl.textContent = `${metrics.latency.toFixed(1)} ms`;
    cpuEl.textContent = `${Math.round(metrics.cpu)}%`;
    memEl.textContent = `${metrics.mem.toFixed(2)} Gb`;

    // Render custom section contents
    customSectionEl.querySelector('h3').textContent = 'Operator Configuration';
    customTextEl.innerHTML = `<p>${config.description}</p><div style="margin-top:10px;">${config.customInfo(metrics)}</div>`;

    // Update Sparkline
    drawSparkline(historyData[selected]);

    // Populate and show explainability card
    const explainCard = document.getElementById('explainability-card');
    if (explainCard) {
      document.getElementById('explain-node-name').textContent = config.name;
      document.getElementById('explain-node-type').textContent = config.type;
      document.getElementById('explain-node-description').textContent = config.description;
      
      const inSample = typeof config.inputSample === 'function' ? config.inputSample() : config.inputSample;
      const outSample = typeof config.outputSample === 'function' ? config.outputSample() : config.outputSample;
      
      document.getElementById('explain-node-input').textContent = inSample;
      document.getElementById('explain-node-output').textContent = outSample;
      
      explainCard.classList.remove('hidden');
    }

  } else {
    // Show overall pipeline stats
    const metrics = metricsData['global'];

    titleEl.textContent = globalPipelineConfig.name;
    badgeEl.textContent = globalPipelineConfig.type;
    badgeEl.className = 'notebook-badge';

    if (simState.isWarningActive) {
      statusEl.innerHTML = '<span style="color:#e03131; font-weight:bold;">⚠️ Degraded</span>';
      badgeEl.className = 'notebook-badge warning-badge';
    } else {
      statusEl.innerHTML = '<span style="color:#2b8a3e; font-weight:bold;">✔ Healthy</span>';
    }

    throughputEl.textContent = `${metrics.throughput.toFixed(2)} M/s`;
    latencyEl.textContent = `${metrics.latency.toFixed(1)} ms`;
    cpuEl.textContent = `${Math.round(metrics.cpu)}%`;
    memEl.textContent = `${metrics.mem.toFixed(2)} Gb`;

    customSectionEl.querySelector('h3').textContent = 'Topology Info';
    customTextEl.innerHTML = `<p>${globalPipelineConfig.description}</p><div style="margin-top:10px;">${globalPipelineConfig.customInfo(metrics)}</div>`;

    // Update Sparkline
    drawSparkline(historyData['global']);

    // Hide explainability card
    const explainCard = document.getElementById('explainability-card');
    if (explainCard) {
      explainCard.classList.add('hidden');
    }
  }
}

// Log message to notebook console
function logToConsole(message, type = 'info') {
  const consoleEl = document.getElementById('log-console');
  if (!consoleEl) return;

  const now = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  const timeStr = `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;

  const entry = document.createElement('div');
  entry.className = `log-entry ${type === 'warn' ? 'log-entry-warn' : ''}`;
  entry.textContent = `[${timeStr}] ${message}`;

  consoleEl.appendChild(entry);
  consoleEl.scrollTop = consoleEl.scrollHeight;

  // Cap logs at 30 entries
  while (consoleEl.children.length > 30) {
    consoleEl.removeChild(consoleEl.firstChild);
  }
}

// Node click selection handler
function setupNodeSelection() {
  const nodes = document.querySelectorAll('.dag-node');
  
  nodes.forEach(node => {
    node.addEventListener('click', (e) => {
      e.stopPropagation(); // prevent background click handler
      
      const nodeId = node.getAttribute('data-node-id');
      
      // Toggle off if already selected
      if (simState.selectedNode === nodeId) {
        deselectAllNodes();
      } else {
        nodes.forEach(n => n.classList.remove('selected'));
        node.classList.add('selected');
        simState.selectedNode = nodeId;
        logToConsole(`Selected node: ${nodesConfig[nodeId].name}`);
        updateInspectorPanel();
        drawConnections();
      }
    });
  });

  // Background click deselects
  document.getElementById('dag-container').addEventListener('click', () => {
    deselectAllNodes();
  });
}

function deselectAllNodes() {
  if (simState.selectedNode === null) return;
  
  const nodes = document.querySelectorAll('.dag-node');
  nodes.forEach(n => n.classList.remove('selected'));
  
  simState.selectedNode = null;
  logToConsole('Reset selection: viewing entire pipeline topology.');
  updateInspectorPanel();
  drawConnections();
}

// Connect toolbar controllers
function setupControls() {
  // Play Pause Toggle
  const btnPlayPause = document.getElementById('btn-play-pause');
  btnPlayPause.addEventListener('click', () => {
    simState.isRunning = !simState.isRunning;
    
    if (simState.isRunning) {
      btnPlayPause.classList.add('active');
      btnPlayPause.innerHTML = '<span class="icon">⏸</span> Pause Sim';
      logToConsole('Simulation resumed.');
      // Redraw particles
      drawConnections();
    } else {
      btnPlayPause.classList.remove('active');
      btnPlayPause.innerHTML = '<span class="icon">▶</span> Resume Sim';
      logToConsole('Simulation paused.');
      // Redraw to remove moving particles
      drawConnections();
    }
  });

  // Inject Warning Toggle
  const btnInjectWarning = document.getElementById('btn-inject-warning');
  const globalIndicator = document.getElementById('global-status-indicator');
  const utenteNode = document.getElementById('node-controlli-utente');
  
  btnInjectWarning.addEventListener('click', () => {
    simState.isWarningActive = !simState.isWarningActive;
    
    if (simState.isWarningActive) {
      btnInjectWarning.innerHTML = '<span class="icon">✨</span> Resolve Warning';
      btnInjectWarning.classList.add('active');
      btnInjectWarning.style.borderColor = '#2b8a3e';
      btnInjectWarning.style.color = '#2b8a3e';
      btnInjectWarning.style.boxShadow = '3px 3px 0px #2b8a3e';

      globalIndicator.className = 'status-indicator-box warning';
      globalIndicator.querySelector('.status-label').textContent = 'Pipeline: WARNING';
      utenteNode.classList.add('warning');

      logToConsole('CRITICAL: Warning injected on Controlli Utente operator.', 'warn');
      logToConsole('WARNING: Latency threshold exceeded for database verify.', 'warn');
    } else {
      btnInjectWarning.innerHTML = '<span class="icon">⚠️</span> Inject Warning';
      btnInjectWarning.classList.remove('active');
      btnInjectWarning.style.borderColor = '#e03131';
      btnInjectWarning.style.color = '#e03131';
      btnInjectWarning.style.boxShadow = '3px 3px 0px #e03131';

      globalIndicator.className = 'status-indicator-box healthy';
      globalIndicator.querySelector('.status-label').textContent = 'Pipeline: HEALTHY';
      utenteNode.classList.remove('warning');

      logToConsole('Simulation alerts resolved. System recovering...');
    }
    
    // Update SVG arrows highlight and sidebar
    updateInspectorPanel();
    drawConnections();
  });

  // Reset/Clear selection
  document.getElementById('btn-clear-selection').addEventListener('click', () => {
    deselectAllNodes();
  });

  // Simulation Speed Slider
  const speedSlider = document.getElementById('simulation-speed');
  const speedVal = document.getElementById('speed-value');
  
  speedSlider.addEventListener('input', (e) => {
    const val = parseFloat(e.target.value);
    simState.speedScale = val;
    speedVal.textContent = `${val.toFixed(1)}x`;
    // logToConsole(`Adjusted speed scale factor to ${val.toFixed(1)}x.`);
    drawConnections();
  });

  // Theme Toggle Button (Light/Dark Neobrutalism)
  const btnThemeToggle = document.getElementById('btn-theme-toggle');
  if (btnThemeToggle) {
    btnThemeToggle.addEventListener('click', () => {
      const isDark = document.body.classList.contains('theme-neobrutalist-dark');
      if (isDark) {
        document.body.className = 'theme-neobrutalist';
        btnThemeToggle.innerHTML = '<span class="icon">🌙</span> Dark Mode';
        logToConsole('Switched to Neobrutalist Light theme.');
      } else {
        document.body.className = 'theme-neobrutalist-dark';
        btnThemeToggle.innerHTML = '<span class="icon">☀️</span> Light Mode';
        logToConsole('Switched to Neobrutalist Dark theme.');
      }
      drawConnections();
    });
  }
}

// Handle window resizing to recalculate connection curves
function setupWindowResize() {
  let resizeTimeout;
  window.addEventListener('resize', () => {
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(() => {
      drawConnections();
    }, 150);
  });
}

// Canvas Panning and Zooming Interaction logic
function setupCanvasPanning() {
  const container = document.getElementById('dag-container');
  const viewport = document.getElementById('dag-viewport');
  if (!container || !viewport) return;
  
  let isPanning = false;
  let startX = 0;
  let startY = 0;
  
  // Mouse down - start panning
  container.addEventListener('mousedown', (e) => {
    // Only drag on empty background space (not on nodes, buttons, inputs)
    if (e.target.closest('.dag-node') || e.target.closest('button') || e.target.closest('input')) {
      return;
    }
    
    isPanning = true;
    container.style.cursor = 'grabbing';
    startX = e.clientX - canvasState.panX;
    startY = e.clientY - canvasState.panY;
    e.preventDefault();
  });
  
  // Mouse move - pan
  window.addEventListener('mousemove', (e) => {
    if (!isPanning) return;
    
    canvasState.panX = e.clientX - startX;
    canvasState.panY = e.clientY - startY;
    
    updateTransform();
  });
  
  // Mouse up - stop panning
  window.addEventListener('mouseup', () => {
    if (isPanning) {
      isPanning = false;
      container.style.cursor = 'grab';
    }
  });
  
  // Mouse wheel zoom
  container.addEventListener('wheel', (e) => {
    e.preventDefault();
    const zoomIntensity = 0.05;
    
    if (e.deltaY < 0) {
      canvasState.scale = Math.min(2.0, canvasState.scale + zoomIntensity);
    } else {
      canvasState.scale = Math.max(0.5, canvasState.scale - zoomIntensity);
    }
    
    updateTransform();
    // Redraw particles to adjust coordinate spacing slightly
    drawConnections();
  }, { passive: false });
  
  // Reset view on double click
  container.addEventListener('dblclick', (e) => {
    if (e.target.closest('.dag-node')) return;
    canvasState.panX = 0;
    canvasState.panY = 0;
    canvasState.scale = 1.0;
    updateTransform();
    drawConnections();
    logToConsole('Reset canvas viewport.');
  });
  
  function updateTransform() {
    viewport.style.transform = `translate(${canvasState.panX}px, ${canvasState.panY}px) scale(${canvasState.scale})`;
  }
}

// Initial Bootstrapping
function init() {
  initMetrics();
  setupNodeSelection();
  setupControls();
  setupCanvasPanning();
  setupWindowResize();

  // Draw initial connections
  // Timeout ensures container has correct sizing layout first
  setTimeout(() => {
    drawConnections();
  }, 100);

  // Set running timer ticker
  simState.tickInterval = setInterval(() => {
    updateMetrics();
  }, 1000);

  logToConsole('Welcome to the Renoir pipeline monitor mockup!');
}

document.addEventListener('DOMContentLoaded', init);
