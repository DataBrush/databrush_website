// Custom Pipeline Builder State
const state = {
  nodes: {},       // id -> { id, type, name, query, x, y }
  links: [],       // Array of { from, to }
  selectedNodeId: null,
  simRunning: true,
  scale: 1.0,
  panX: 0,
  panY: 0,
  connectingSourceId: null,
  isDraggingCanvas: false,
  isDrawingConnection: false,
  metricsInterval: null,
  historyLength: 20,
  metricsHistory: {}, // nodeId -> Array of throughput
  logEntries: []
};

// Check language
const isIt = window.location.pathname.includes('/it/');

// Localized strings
const L = {
  sourceQuestion: isIt ? "Da dove vuoi prendere i dati?" : "Where do you want to take the data?",
  filterQuestion: isIt ? "Cosa vuoi filtrare?" : "What do you want to filter?",
  mapQuestion: isIt ? "Come vuoi trasformare i dati?" : "How do you want to transform the data?",
  windowQuestion: isIt ? "Quale finestra temporale vuoi impostare?" : "What window size do you want?",
  reduceQuestion: isIt ? "Come vuoi ridurre o aggregare i dati?" : "How do you want to aggregate/reduce the data?",
  sinkQuestion: isIt ? "Dove vuoi scrivere i dati?" : "Where do you want to write the data?",
  
  sourceDefault: isIt ? "Sorgente Kafka" : "Kafka Source",
  filterDefault: isIt ? "Filtro Valori" : "Value Filter",
  mapDefault: isIt ? "Trasformatore" : "Map Operator",
  windowDefault: isIt ? "Finestra Temporale" : "Time Window",
  reduceDefault: isIt ? "Aggregatore" : "Reducer",
  sinkDefault: isIt ? "Database Postgres" : "Postgres Sink",

  logWelcome: isIt ? "Benvenuto nel costruttore di pipeline Renoir!" : "Welcome to the Renoir custom pipeline builder!",
  logNodeAdded: (type, name) => isIt ? `Aggiunto operatore ${type}: ${name}` : `Added ${type} operator: ${name}`,
  logNodeDeleted: (name) => isIt ? `Eliminato operatore: ${name}` : `Deleted operator: ${name}`,
  logConnected: (from, to) => isIt ? `Collegato ${from} a ${to}` : `Connected ${from} to ${to}`,
  logDisconnected: (from, to) => isIt ? `Scollegato ${from} da ${to}` : `Disconnected ${from} from ${to}`,
  logCleared: isIt ? "Canvas svuotato." : "Workspace cleared.",
  logResetView: isIt ? "Vista resettata." : "Viewport view reset.",
  logDuplicateLink: isIt ? "Collegamento già esistente!" : "Connection already exists!",
  logLoopAlert: isIt ? "Attenzione: Impossibile creare cicli!" : "Warning: Loops are not allowed in this pipeline!",

  // Preset values for quick buttons
  presets: {
    source: [
      { label: isIt ? "Kafka Topic" : "Kafka Topic", val: "transactions-stream" },
      { label: isIt ? "Sensori IoT" : "IoT Sensors", val: "sensor-telemetry" },
      { label: isIt ? "Webhook HTTP" : "HTTP Webhook", val: "/api/v1/ingest" },
      { label: isIt ? "Secchio S3" : "S3 Bucket", val: "s3://raw-incoming-events" }
    ],
    filter: [
      { label: "amount > 100", val: "amount > 100.0" },
      { label: "is_verified", val: "user.is_verified == true" },
      { label: "status == 'ERR'", val: "status == \"ERROR\"" },
      { label: "temp > 37.5", val: "temperature > 37.5" }
    ],
    map: [
      { label: "curr -> USD", val: "convert_currency(amount, \"USD\")" },
      { label: "anonymize", val: "anonymize_user_id(user_id)" },
      { label: "add_metadata", val: "add_processing_metadata(event)" },
      { label: "parse_json", val: "parse_json_payload(raw_payload)" }
    ],
    window: [
      { label: "Tumbling 10s", val: "TumblingWindow(Duration::from_secs(10))" },
      { label: "Sliding 1m/10s", val: "SlidingWindow(Duration::from_secs(60), Duration::from_secs(10))" },
      { label: "Count 100", val: "CountWindow(100)" }
    ],
    reduce: [
      { label: isIt ? "Somma" : "Sum", val: "|a, b| a + b" },
      { label: isIt ? "Massimo" : "Max", val: "|a, b| std::cmp::max(a, b)" },
      { label: isIt ? "Media" : "Average", val: "|a, b| (a + b) / 2.0" }
    ],
    sink: [
      { label: "PostgreSQL", val: "postgres-audit-cluster" },
      { label: "AWS S3", val: "s3://processed-archived-data" },
      { label: "Slack Notify", val: "slack-notifications-webhook" },
      { label: "ElasticSearch", val: "elasticsearch-indexing-service" }
    ]
  }
};

// Log event helper
function logToConsole(message) {
  const consoleEl = document.getElementById('builder-log-console');
  if (!consoleEl) return;
  const time = new Date().toLocaleTimeString();
  const entry = document.createElement('div');
  entry.className = 'log-entry border-b border-gray-100 pb-0.5';
  entry.innerHTML = `<span class="text-gray-400 font-mono">[${time}]</span> ${message}`;
  consoleEl.appendChild(entry);
  consoleEl.scrollTop = consoleEl.scrollHeight;
}

// Transform matrix updater
function updateCanvasTransform() {
  const viewport = document.getElementById('dag-viewport');
  const indicator = document.getElementById('canvas-zoom-indicator');
  if (viewport) {
    viewport.style.transform = `translate(${state.panX}px, ${state.panY}px) scale(${state.scale})`;
  }
  if (indicator) {
    indicator.textContent = `Zoom: ${Math.round(state.scale * 100)}% | Pan: ${Math.round(state.panX)},${Math.round(state.panY)}`;
  }
}

// Drag & Drop Setup for palette items
function setupDragAndDrop() {
  const container = document.getElementById('dag-container');
  const viewport = document.getElementById('dag-viewport');
  const paletteItems = document.querySelectorAll('.palette-item');

  paletteItems.forEach(item => {
    item.addEventListener('dragstart', (e) => {
      e.dataTransfer.setData('text/plain', item.getAttribute('data-operator-type'));
      e.dataTransfer.effectAllowed = 'copy';
    });
  });

  container.addEventListener('dragover', (e) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'copy';
  });

  container.addEventListener('drop', (e) => {
    e.preventDefault();
    const type = e.dataTransfer.getData('text/plain');
    if (!['source', 'filter', 'map', 'sink', 'window', 'reduce'].includes(type)) return;

    // Calculate drop coordinates adjusted for zoom and pan
    const rect = viewport.getBoundingClientRect();
    const dropX = (e.clientX - rect.left) / state.scale;
    const dropY = (e.clientY - rect.top) / state.scale;

    openConfigModal(type, dropX, dropY);
  });
}

// Open operator configuration dialog modal
function openConfigModal(type, x, y, editId = null) {
  const modal = document.getElementById('operator-config-modal');
  const modalTitle = document.getElementById('modal-title-label');
  const modalSubtitle = document.getElementById('modal-subtitle-label');
  const opTypeInput = document.getElementById('modal-op-type');
  const dropXInput = document.getElementById('modal-drop-x');
  const dropYInput = document.getElementById('modal-drop-y');
  const editIdInput = document.getElementById('modal-edit-id');
  const opNameInput = document.getElementById('modal-op-name');
  const opQueryInput = document.getElementById('modal-op-query');
  const queryPrompt = document.getElementById('modal-question-prompt');
  const presetsContainer = document.getElementById('modal-presets-container');

  // Fill hidden parameters
  opTypeInput.value = type;
  dropXInput.value = x;
  dropYInput.value = y;
  editIdInput.value = editId || '';

  // Setup presets list
  presetsContainer.innerHTML = '';
  const opPresets = L.presets[type] || [];
  opPresets.forEach(preset => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'sketch-btn font-mono text-[10px] px-2 py-1 border border-black bg-white hover:bg-yellow-100 transition-colors select-none cursor-pointer';
    btn.textContent = preset.label;
    btn.addEventListener('click', () => {
      opQueryInput.value = preset.val;
      opQueryInput.focus();
    });
    presetsContainer.appendChild(btn);
  });

  // Customize prompt depending on operator type
  if (editId) {
    const existingNode = state.nodes[editId];
    opNameInput.value = existingNode.name;
    opQueryInput.value = existingNode.query;
    modalTitle.textContent = isIt ? "Modifica Operatore" : "Edit Operator";
    modalSubtitle.textContent = isIt ? "Aggiorna i parametri di calcolo" : "Modify configuration parameters";
  } else {
    opNameInput.value = L[`${type}Default`] + " " + (Object.keys(state.nodes).length + 1);
    opQueryInput.value = opPresets[0] ? opPresets[0].val : '';
    modalTitle.textContent = isIt ? "Configura Operatore" : "Configure Operator";
    modalSubtitle.textContent = isIt ? "Inserisci i parametri per iniziare il calcolo" : "Enter requirements to define the operator";
  }

  // Question custom query labels
  queryPrompt.textContent = L[`${type}Question`];
  opQueryInput.placeholder = 
    type === 'source' ? 'e.g. topic-name' : 
    type === 'filter' ? 'e.g. x > 5' : 
    type === 'map' ? 'e.g. x * 2' : 
    type === 'window' ? 'e.g. TumblingWindow(10s)' :
    type === 'reduce' ? 'e.g. |a, b| a + b' :
    'e.g. database-endpoint';

  // Open modal
  modal.classList.remove('hidden');
  modal.classList.add('flex');
  opNameInput.focus();
}

function closeConfigModal() {
  const modal = document.getElementById('operator-config-modal');
  modal.classList.remove('flex');
  modal.classList.add('hidden');
}

// Generate code snippet for single operator
function getRustSnippet(node) {
  const query = node.query || "...";
  switch (node.type) {
    case 'source':
      return `let stream = ctx.stream_kafka(kafka_conf, &["${query}"], Replication::Unlimited);`;
    case 'filter':
      return `.filter(|event| event.${query})`;
    case 'map':
      return `.map(|event| event.${query})`;
    case 'window':
      return `.window(${query})`;
    case 'reduce':
      return `.reduce(${query})`;
    case 'sink':
      return `.sink(PostgresSink::new("${query}"));`;
    default:
      return '';
  }
}



// Select operator node
function selectNode(nodeId) {
  state.selectedNodeId = nodeId;
  
  const titleEl = document.getElementById('inspector-title');
  const badgeEl = document.getElementById('inspector-badge');
  const globalState = document.getElementById('inspector-global-state');
  const nodeState = document.getElementById('inspector-node-state');

  // Highlight node element on canvas
  document.querySelectorAll('.dag-node').forEach(el => el.classList.remove('selected'));
  if (nodeId) {
    const nodeEl = document.getElementById(`node-${nodeId}`);
    if (nodeEl) nodeEl.classList.add('selected');

    const node = state.nodes[nodeId];
    titleEl.textContent = node.name;
    let badgeClass = 'bg-color-chart-3 text-black';
    if (node.type === 'source') badgeClass = 'bg-color-chart-2 text-black';
    else if (node.type === 'filter') badgeClass = 'bg-color-chart-1 text-black';
    else if (node.type === 'map') badgeClass = 'bg-color-chart-5 text-white';
    else if (node.type === 'window') badgeClass = 'bg-color-chart-4 text-white';
    else if (node.type === 'reduce') badgeClass = 'bg-white text-black';
    else if (node.type === 'sink') badgeClass = 'bg-color-chart-3 text-black';

    badgeEl.className = `notebook-badge ${badgeClass} font-black text-xs uppercase px-2 py-0.5 border-2 border-black`;

    // Fill inspector inputs
    document.getElementById('selected-node-name-input').value = node.name;
    document.getElementById('selected-node-type-label').textContent = node.type.toUpperCase();
    
    // Customize parameter label
    const paramLabel = document.getElementById('selected-node-param-label');
    paramLabel.textContent = 
      node.type === 'source' ? (isIt ? "Indirizzo Origine" : "Source Ingest") 
      : node.type === 'filter' ? (isIt ? "Criterio Filtro" : "Filter Criteria")
      : node.type === 'map' ? (isIt ? "Formula Mappa" : "Map Formula")
      : node.type === 'window' ? (isIt ? "Configurazione Finestra" : "Window Configuration")
      : node.type === 'reduce' ? (isIt ? "Formula Riduzione" : "Reduce Formula")
      : (isIt ? "Destinazione Record" : "Target Output");

    document.getElementById('selected-node-param-input').value = node.query;
    document.getElementById('selected-node-rust-code').textContent = getRustSnippet(node);

    globalState.classList.add('hidden');
    nodeState.classList.remove('hidden');
  } else {
    // Show global statistics
    titleEl.textContent = isIt ? "Analisi Pipeline" : "Global Pipeline";
    badgeEl.textContent = isIt ? "STRUTTURA" : "GRAPH";
    badgeEl.className = "notebook-badge bg-color-chart-3 text-black font-black text-xs uppercase px-2 py-0.5 border-2 border-black";
    
    document.getElementById('stat-operator-count').textContent = Object.keys(state.nodes).length;
    document.getElementById('stat-connection-count').textContent = state.links.length;

    nodeState.classList.add('hidden');
    globalState.classList.remove('hidden');
  }
}

// Draw HTML operators inside the viewport
function renderNodes() {
  const viewport = document.getElementById('dag-viewport');
  if (!viewport) return;

  // Clear existing nodes but keep connections SVG and temp paths
  viewport.querySelectorAll('.dag-node').forEach(node => node.remove());

  Object.keys(state.nodes).forEach(id => {
    const node = state.nodes[id];
    
    const nodeEl = document.createElement('div');
    nodeEl.className = `dag-node absolute select-none ${state.selectedNodeId === id ? 'selected' : ''}`;
    nodeEl.id = `node-${id}`;
    nodeEl.style.left = node.x + 'px';
    nodeEl.style.top = node.y + 'px';

    // Render neobrutalist outline card
    let innerHTML = `
      <div class="node-border"></div>
      <div class="node-content">
        <span class="node-type font-mono text-[10px] text-gray-500 dark:text-gray-400">${node.type.toUpperCase()}</span>
        <span class="node-title select-none font-bold text-sm block truncate max-w-[140px]">${node.name}</span>
      </div>
      <div class="node-sketch-bubble">
        <div class="bubble-border"></div>
        <div class="bubble-content font-bold text-xs select-none">
          <div>throughput: <span class="bubble-stat text-green-600" id="bubble-${id}-tput">...</span></div>
          <div>CPU: <span class="bubble-stat font-mono text-black" id="bubble-${id}-cpu">...</span></div>
          <div>Mem: <span class="bubble-stat font-mono text-black" id="bubble-${id}-mem">...</span></div>
        </div>
      </div>
    `;

    nodeEl.innerHTML = innerHTML;

    // Output Port (Green) on Right: for everything except Sink
    if (node.type !== 'sink') {
      const outPort = document.createElement('div');
      outPort.className = 'node-port output-port';
      outPort.setAttribute('data-port-type', 'output');
      outPort.setAttribute('data-node-id', id);
      nodeEl.appendChild(outPort);

      // Event listener for starting connections
      outPort.addEventListener('mousedown', (e) => {
        e.stopPropagation();
        e.preventDefault();
        startDrawingConnection(id);
      });
      outPort.addEventListener('click', (e) => {
        e.stopPropagation();
        clickTogglePort(id);
      });
    }

    // Input Port (Purple) on Left: for everything except Source
    if (node.type !== 'source') {
      const inPort = document.createElement('div');
      inPort.className = 'node-port input-port';
      inPort.setAttribute('data-port-type', 'input');
      inPort.setAttribute('data-node-id', id);
      nodeEl.appendChild(inPort);

      // Stop event propagation for mousedown to prevent triggering node dragging
      inPort.addEventListener('mousedown', (e) => {
        e.stopPropagation();
      });

      // Mouse release for drag connection drop
      inPort.addEventListener('mouseup', (e) => {
        e.stopPropagation();
        if (state.isDrawingConnection && state.connectingSourceId) {
          completeDragConnection(state.connectingSourceId, id);
        }
      });
      inPort.addEventListener('click', (e) => {
        e.stopPropagation();
        clickTogglePort(id, true);
      });
    }

    // Node click handlers
    nodeEl.addEventListener('click', (e) => {
      e.stopPropagation();
      selectNode(id);
    });

    // Double click to edit parameters
    nodeEl.addEventListener('dblclick', (e) => {
      e.stopPropagation();
      openConfigModal(node.type, node.x, node.y, id);
    });

    // Node drag handling
    nodeEl.addEventListener('mousedown', (e) => {
      // Don't drag if we clicked a port
      if (e.target.closest('.node-port')) return;

      selectNode(id);

      let isDraggingNode = true;
      const startMouseX = e.clientX;
      const startMouseY = e.clientY;
      const startNodeX = node.x;
      const startNodeY = node.y;

      const onMouseMove = (moveEvent) => {
        if (!isDraggingNode) return;
        const dx = (moveEvent.clientX - startMouseX) / state.scale;
        const dy = (moveEvent.clientY - startMouseY) / state.scale;

        node.x = startNodeX + dx;
        node.y = startNodeY + dy;

        nodeEl.style.left = node.x + 'px';
        nodeEl.style.top = node.y + 'px';

        drawConnections();
      };

      const onMouseUp = () => {
        isDraggingNode = false;
        window.removeEventListener('mousemove', onMouseMove);
        window.removeEventListener('mouseup', onMouseUp);
      };

      window.addEventListener('mousemove', onMouseMove);
      window.addEventListener('mouseup', onMouseUp);
      
      e.stopPropagation();
    });

    viewport.appendChild(nodeEl);
  });

  // Ensure stats reflect initial status
  updateNodeBubbleStats();
}

// Click port connection logic
function clickTogglePort(nodeId, isInput = false) {
  if (!isInput) {
    // Started output port click
    if (state.connectingSourceId === nodeId) {
      // toggle off
      cancelConnectingState();
    } else {
      cancelConnectingState();
      state.connectingSourceId = nodeId;
      const port = document.querySelector(`#node-${nodeId} .output-port`);
      if (port) port.classList.add('connecting-active');
      logToConsole(isIt ? "Selezionata sorgente. Clicca sulla porta viola di destinazione." : "Source selected. Click a target purple port to link.");
    }
  } else {
    // Clicked target input port
    if (state.connectingSourceId) {
      if (state.connectingSourceId === nodeId) return; // can't connect to self
      
      const from = state.connectingSourceId;
      const to = nodeId;
      
      createLink(from, to);
      cancelConnectingState();
    }
  }
}

function cancelConnectingState() {
  state.connectingSourceId = null;
  document.querySelectorAll('.node-port').forEach(p => p.classList.remove('connecting-active'));
}

// Drag connection drawing triggers
function startDrawingConnection(fromNodeId) {
  state.isDrawingConnection = true;
  state.connectingSourceId = fromNodeId;
  
  const tempPath = document.getElementById('temp-connection-path');
  if (tempPath) tempPath.style.display = 'block';

  // Add temp mouse listener for SVG line following
  window.addEventListener('mousemove', dragConnectionMouseMove);
  window.addEventListener('mouseup', endDragConnectionMouseUp);
}

function dragConnectionMouseMove(e) {
  if (!state.isDrawingConnection || !state.connectingSourceId) return;

  const viewport = document.getElementById('dag-viewport');
  const tempPath = document.getElementById('temp-connection-path');
  if (!viewport || !tempPath) return;

  const rect = viewport.getBoundingClientRect();
  const mouseX = (e.clientX - rect.left) / state.scale;
  const mouseY = (e.clientY - rect.top) / state.scale;

  const fromNode = state.nodes[state.connectingSourceId];
  if (!fromNode) return;

  // From center-right output port
  const x1 = fromNode.x + 87.5;
  const y1 = fromNode.y;

  const dx = Math.abs(mouseX - x1) * 0.45;
  const d = `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${mouseX - dx} ${mouseY}, ${mouseX} ${mouseY}`;
  tempPath.setAttribute('d', d);
}

function endDragConnectionMouseUp(e) {
  window.removeEventListener('mousemove', dragConnectionMouseMove);
  window.removeEventListener('mouseup', endDragConnectionMouseUp);

  const tempPath = document.getElementById('temp-connection-path');
  if (tempPath) {
    tempPath.style.display = 'none';
    tempPath.setAttribute('d', '');
  }

  // If we didn't land directly on a port (handled by input port's mouseup listener), cancel
  setTimeout(() => {
    if (state.isDrawingConnection) {
      state.isDrawingConnection = false;
      state.connectingSourceId = null;
    }
  }, 50);
}

function completeDragConnection(fromId, toId) {
  state.isDrawingConnection = false;
  state.connectingSourceId = null;
  createLink(fromId, toId);
}

// Create connection link between nodes
function createLink(from, to) {
  // 1. Prevent duplicate link
  const duplicate = state.links.some(l => l.from === from && l.to === to);
  if (duplicate) {
    logToConsole(L.logDuplicateLink);
    return;
  }

  // 2. Prevent self connection
  if (from === to) return;

  // 3. Prevent simple circular loops to keep DAG validation clean
  if (checkPathExists(to, from)) {
    logToConsole(L.logLoopAlert);
    return;
  }

  state.links.push({ from, to });
  
  const fromName = state.nodes[from].name;
  const toName = state.nodes[to].name;
  logToConsole(L.logConnected(fromName, toName));

  drawConnections();
  selectNode(state.selectedNodeId); // refresh stats count
}

// Simple path check to validate DAG (checks if destination points back to source)
function checkPathExists(startNodeId, targetNodeId, visited = new Set()) {
  if (startNodeId === targetNodeId) return true;
  visited.add(startNodeId);
  const nextNodes = state.links.filter(l => l.from === startNodeId).map(l => l.to);
  for (const nextNodeId of nextNodes) {
    if (!visited.has(nextNodeId)) {
      if (checkPathExists(nextNodeId, targetNodeId, visited)) {
        return true;
      }
    }
  }
  return false;
}

// Draw connection paths
function drawConnections() {
  const svg = document.getElementById('connections-svg');
  if (!svg) return;

  // Remove existing paths
  svg.querySelectorAll('.connection-path, .particle-group').forEach(el => el.remove());

  state.links.forEach((link, index) => {
    const fromNode = state.nodes[link.from];
    const toNode = state.nodes[link.to];
    if (!fromNode || !toNode) return;

    // Local coordinates matching node dimensions
    const x1 = fromNode.x + 87.5;
    const y1 = fromNode.y;
    const x2 = toNode.x - 87.5;
    const y2 = toNode.y;

    const dx = Math.abs(x2 - x1) * 0.45;
    const d = `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`;

    // Create Path
    const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
    path.setAttribute('d', d);
    
    // Highlight if selected node is endpoints of link
    let isActive = false;
    if (state.selectedNodeId && (state.selectedNodeId === link.from || state.selectedNodeId === link.to)) {
      isActive = true;
    }

    path.setAttribute('class', `connection-path ${isActive ? 'active' : ''}`);
    const markerId = isActive ? 'arrow-green' : 'arrow-black';
    path.setAttribute('marker-end', `url(#${markerId})`);

    // Click path to delete it
    path.addEventListener('click', (e) => {
      e.stopPropagation();
      deleteLink(index);
    });

    svg.appendChild(path);

    // Spawn floating bubbles if simulation is running
    if (state.simRunning) {
      const duration = '2.2s';
      for (let i = 0; i < 2; i++) {
        const delay = (i * 1.1) + 's';

        const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        g.setAttribute('class', 'particle-group');

        const bubble = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        bubble.setAttribute('r', '5.5');
        bubble.setAttribute('class', 'particle-bubble');
        if (isActive) {
          bubble.style.stroke = 'var(--chart-2)';
          bubble.style.fill = '#ebfbee';
        }
        g.appendChild(bubble);

        const glint = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        glint.setAttribute('cx', '-1.5');
        glint.setAttribute('cy', '-1.5');
        glint.setAttribute('r', '1.2');
        glint.setAttribute('class', 'particle-glint');
        g.appendChild(glint);

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

function deleteLink(index) {
  const link = state.links[index];
  if (!link) return;

  const fromName = state.nodes[link.from].name;
  const toName = state.nodes[link.to].name;
  logToConsole(L.logDisconnected(fromName, toName));

  state.links.splice(index, 1);
  drawConnections();
  selectNode(state.selectedNodeId);
}

// Simulation values calculator
function updateNodeBubbleStats() {
  Object.keys(state.nodes).forEach(id => {
    const node = state.nodes[id];
    
    // Set base values
    let tput = 0;
    let cpu = 0;
    let mem = 0;

    if (state.simRunning) {
      if (node.type === 'source') {
        tput = (5.0 + Math.random() * 3.0).toFixed(1);
        cpu = Math.round(5 + Math.random() * 8);
        mem = (1.2 + Math.random() * 0.4).toFixed(1);
      } else {
        // Inherit from parent sources
        const incomingLinks = state.links.filter(l => l.to === id);
        if (incomingLinks.length === 0) {
          tput = "0.0";
          cpu = Math.round(1 + Math.random() * 2);
          mem = (0.2 + Math.random() * 0.1).toFixed(1);
        } else {
          // Average incoming throughputs
          let sumTput = 0;
          incomingLinks.forEach(l => {
            const parentBubble = document.getElementById(`bubble-${l.from}-tput`);
            const val = parentBubble ? parseFloat(parentBubble.textContent) : 0;
            sumTput += val;
          });
          
          let resultTput = sumTput / incomingLinks.length;
          // Filters/Aggregation reduce throughput, maps process it
          if (node.type === 'filter') {
            resultTput *= 0.45; // filter drops 55%
          } else if (node.type === 'reduce') {
            resultTput *= 0.10; // reduce aggregates to 10%
          } else if (node.type === 'window') {
            resultTput *= 0.95; // window has slight buffering overhead
          } else {
            resultTput *= 0.98; // map/sink overhead drops 2%
          }
          tput = resultTput.toFixed(1);
          cpu = Math.round(8 + Math.random() * 10);
          mem = (1.5 + Math.random() * 0.8).toFixed(1);
        }
      }
    } else {
      tput = "0.0";
      cpu = 0;
      mem = "0.1";
    }

    const tputEl = document.getElementById(`bubble-${id}-tput`);
    const cpuEl = document.getElementById(`bubble-${id}-cpu`);
    const memEl = document.getElementById(`bubble-${id}-mem`);

    if (tputEl) tputEl.textContent = `${tput}M/s`;
    if (cpuEl) cpuEl.textContent = `${cpu}%`;
    if (memEl) memEl.textContent = `${mem} Gb`;
  });
}

// Reset workspace
function clearWorkspace() {
  state.nodes = {};
  state.links = [];
  state.selectedNodeId = null;
  
  logToConsole(L.logCleared);
  
  renderNodes();
  drawConnections();
  selectNode(null);
}

// Drag & zoom canvas panning setup
function setupCanvasPanning() {
  const container = document.getElementById('dag-container');
  const viewport = document.getElementById('dag-viewport');
  if (!container || !viewport) return;

  let startX = 0;
  let startY = 0;

  container.addEventListener('mousedown', (e) => {
    // If clicking a port, button, input, or node, ignore panning trigger
    if (e.target.closest('.dag-node') || e.target.closest('button') || e.target.closest('input') || e.target.closest('.node-port')) {
      return;
    }

    state.isDraggingCanvas = true;
    container.style.cursor = 'grabbing';
    startX = e.clientX - state.panX;
    startY = e.clientY - state.panY;
    e.preventDefault();
  });

  window.addEventListener('mousemove', (e) => {
    if (!state.isDraggingCanvas) return;
    
    state.panX = e.clientX - startX;
    state.panY = e.clientY - startY;
    updateCanvasTransform();
  });

  window.addEventListener('mouseup', () => {
    if (state.isDraggingCanvas) {
      state.isDraggingCanvas = false;
      container.style.cursor = 'grab';
    }
  });

  container.addEventListener('wheel', (e) => {
    e.preventDefault();
    const zoomFactor = 0.05;
    if (e.deltaY < 0) {
      state.scale = Math.min(2.0, state.scale + zoomFactor);
    } else {
      state.scale = Math.max(0.5, state.scale - zoomFactor);
    }
    updateCanvasTransform();
  }, { passive: false });

  // Double click resets pan and scale
  container.addEventListener('dblclick', (e) => {
    if (e.target.closest('.dag-node') || e.target.closest('.node-port')) return;
    state.panX = 0;
    state.panY = 0;
    state.scale = 1.0;
    updateCanvasTransform();
    logToConsole(L.logResetView);
  });
}

// Delete operator
function deleteSelectedNode() {
  const nodeId = state.selectedNodeId;
  if (!nodeId || !state.nodes[nodeId]) return;

  const nodeName = state.nodes[nodeId].name;

  // 1. Delete links ending or starting here
  state.links = state.links.filter(l => l.from !== nodeId && l.to !== nodeId);

  // 2. Remove node from state
  delete state.nodes[nodeId];
  logToConsole(L.logNodeDeleted(nodeName));

  // 3. Redraw
  state.selectedNodeId = null;
  renderNodes();
  drawConnections();
  selectNode(null);
}

// Save operator modifications from inspector
function saveInspectorModifications() {
  const nodeId = state.selectedNodeId;
  if (!nodeId || !state.nodes[nodeId]) return;

  const name = document.getElementById('selected-node-name-input').value.trim();
  const query = document.getElementById('selected-node-param-input').value.trim();

  if (!name || !query) return;

  state.nodes[nodeId].name = name;
  state.nodes[nodeId].query = query;

  renderNodes();
  selectNode(nodeId);
}

// Initial bootstrap
function init() {
  logToConsole(L.logWelcome);

  // Setup form submission in config modal
  const form = document.getElementById('operator-config-form');
  form.addEventListener('submit', (e) => {
    e.preventDefault();

    const type = document.getElementById('modal-op-type').value;
    const x = parseFloat(document.getElementById('modal-drop-x').value);
    const y = parseFloat(document.getElementById('modal-drop-y').value);
    const editId = document.getElementById('modal-edit-id').value;

    const name = document.getElementById('modal-op-name').value.trim();
    const query = document.getElementById('modal-op-query').value.trim();

    if (!name || !query) return;

    if (editId && state.nodes[editId]) {
      // Modify existing node
      state.nodes[editId].name = name;
      state.nodes[editId].query = query;
      logToConsole(isIt ? `Aggiornato operatore: ${name}` : `Updated operator: ${name}`);
    } else {
      // Create new node
      const id = 'node_' + Date.now();
      state.nodes[id] = { id, type, name, query, x, y };
      logToConsole(L.logNodeAdded(type.toUpperCase(), name));
    }

    closeConfigModal();
    renderNodes();
    drawConnections();
    selectNode(editId || null);
  });

  // Cancel modal buttons
  document.getElementById('btn-modal-cancel').addEventListener('click', closeConfigModal);
  document.getElementById('modal-backdrop').addEventListener('click', closeConfigModal);

  // Panning reset on click background
  const container = document.getElementById('dag-container');
  container.addEventListener('click', (e) => {
    if (e.target === container || e.target.id === 'dag-viewport' || e.target.id === 'connections-svg') {
      selectNode(null);
      cancelConnectingState();
    }
  });

  // Play Pause Controls
  const btnPlaySim = document.getElementById('btn-play-pause-builder');
  if (btnPlaySim) {
    btnPlaySim.addEventListener('click', () => {
      state.simRunning = !state.simRunning;
      const statusVal = document.getElementById('stat-sim-status');

      if (state.simRunning) {
        btnPlaySim.classList.add('active');
        btnPlaySim.innerHTML = isIt ? '<span class="icon">⏸</span> Pausa Simulazione' : '<span class="icon">⏸</span> Pause Sim';
        if (statusVal) statusVal.textContent = isIt ? 'Attivo (Demo)' : 'Running (Demo)';
        statusVal.className = 'value font-black text-sm text-green-600';
      } else {
        btnPlaySim.classList.remove('active');
        btnPlaySim.innerHTML = isIt ? '<span class="icon">▶</span> Avvia Simulazione' : '<span class="icon">▶</span> Resume Sim';
        if (statusVal) statusVal.textContent = isIt ? 'Pausa' : 'Paused';
        statusVal.className = 'value font-black text-sm text-red-600';
      }
      drawConnections();
    });
  }

  // Clear workspace button
  document.getElementById('btn-clear-canvas').addEventListener('click', clearWorkspace);

  // Inspector Action bindings
  document.getElementById('btn-save-selected-node').addEventListener('click', saveInspectorModifications);
  document.getElementById('btn-delete-selected-node').addEventListener('click', deleteSelectedNode);

  // Run initial state setups
  setupDragAndDrop();
  setupCanvasPanning();
  updateCanvasTransform();
  selectNode(null);

  // Download, Import and Consultation buttons
  const btnDownload = document.getElementById('btn-download-pipeline');
  if (btnDownload) {
    btnDownload.addEventListener('click', downloadPipeline);
  }

  const btnImport = document.getElementById('btn-import-pipeline');
  const fileInput = document.getElementById('import-pipeline-file-input');
  if (btnImport && fileInput) {
    btnImport.addEventListener('click', () => fileInput.click());
    fileInput.addEventListener('change', importPipeline);
  }
  
  const btnConsult = document.getElementById('btn-consult-pipeline');
  if (btnConsult) {
    btnConsult.addEventListener('click', requestConsultation);
  }
  
  const btnConsultClose = document.getElementById('btn-consult-close');
  if (btnConsultClose) {
    btnConsultClose.addEventListener('click', closeConsultationModal);
  }
  
  const backdropConsult = document.getElementById('consult-modal-backdrop');
  if (backdropConsult) {
    backdropConsult.addEventListener('click', closeConsultationModal);
  }

  // Metrics intervals ticks (simulated)
  state.metricsInterval = setInterval(() => {
    updateNodeBubbleStats();
  }, 1000);
}

// Download pipeline configuration layout JSON
function downloadPipeline() {
  const pipelineData = {
    nodes: state.nodes,
    links: state.links,
    exportedAt: new Date().toISOString(),
    generator: "Renoir Pipeline Builder"
  };
  const jsonStr = JSON.stringify(pipelineData, null, 2);
  const blob = new Blob([jsonStr], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `renoir-custom-pipeline-${Date.now()}.json`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
  logToConsole(isIt ? "Configurazione pipeline scaricata correttamente." : "Pipeline layout JSON downloaded successfully.");
}

// Import pipeline configuration layout JSON from local disk
function importPipeline(e) {
  const file = e.target.files[0];
  if (!file) return;

  const reader = new FileReader();
  reader.onload = function(evt) {
    try {
      const data = JSON.parse(evt.target.result);
      if (!data.nodes || !data.links) {
        throw new Error(isIt ? "Formato non valido: mancano nodi o collegamenti." : "Invalid format: missing nodes or links.");
      }
      
      // Load workspace state
      state.nodes = data.nodes;
      state.links = data.links;
      state.selectedNodeId = null;

      // Re-render components
      renderNodes();
      drawConnections();
      selectNode(null);
      
      logToConsole(isIt ? "Configurazione pipeline importata correttamente." : "Pipeline layout JSON imported successfully.");
    } catch (err) {
      alert((isIt ? "Errore importazione: " : "Import error: ") + err.message);
      logToConsole((isIt ? "Errore caricamento: " : "Error loading import file: ") + err.message);
    }
    // Reset file input value to allow re-importing the same file
    e.target.value = '';
  };
  reader.readAsText(file);
}

// Engineers consultation request modal triggers
function requestConsultation() {
  // 1. Download pipeline.json
  const pipelineData = {
    nodes: state.nodes,
    links: state.links,
    exportedAt: new Date().toISOString(),
    generator: "Renoir Pipeline Builder"
  };
  const jsonStr = JSON.stringify(pipelineData, null, 2);
  const blob = new Blob([jsonStr], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'pipeline.json';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);

  // 2. Open mail client with prefilled fields
  const to = 'info@databrush.it';
  const subject = encodeURIComponent(isIt ? 'Richiesta di Consulenza Pipeline Renoir' : 'Renoir Pipeline Consultation Request');
  const body = encodeURIComponent(isIt 
    ? 'Gentile Team di Databrush,\n\nVorrei richiedere una consulenza per la mia pipeline personalizzata Renoir.\nHo allegato a questa email il file "pipeline.json" esportato dal costruttore.\n\nCordiali saluti,\n[Mio Nome]'
    : 'Dear Databrush Team,\n\nI would like to request a consultation for my custom Renoir pipeline.\nI have attached the exported "pipeline.json" file to this email.\n\nBest regards,\n[My Name]');
  
  const mailtoLink = `mailto:${to}?subject=${subject}&body=${body}`;
  window.location.href = mailtoLink;

  // 3. Show instructions modal
  const modal = document.getElementById('consultation-modal');
  if (modal) {
    modal.classList.remove('hidden');
    modal.classList.add('flex');
  }
  logToConsole(isIt ? "Configurazione scaricata e client mail aperto." : "Pipeline configuration downloaded and email client opened.");
}

function closeConsultationModal() {
  const modal = document.getElementById('consultation-modal');
  if (modal) {
    modal.classList.remove('flex');
    modal.classList.add('hidden');
  }
}

document.addEventListener('DOMContentLoaded', init);
