// State Management
const simState = {
  isRunning: true,
  speedScale: 1.0,
  isWarningActive: false,
  selectedNode: null, // null means overall pipeline
  tickInterval: null,
  historyLength: 20,
  currentScenario: 'financial'
};

// Canvas Pan & Zoom State
const canvasState = {
  panX: 0,
  panY: 0,
  scale: 1.0
};

// Detect Language
const currentLang = document.documentElement.lang || 'en';
const isIt = currentLang === 'it';

// Active runtime metrics values
const metricsData = {};

// History arrays for sparkline charts
const historyData = {
  global: []
};

// Dynamically resolved settings pointing to the current scenario
let nodesConfig = {};
let globalPipelineConfig = {};

// Scenario configurations
const scenarios = {
  financial: {
    name: isIt ? 'Audit Finanziario' : 'Financial Audit',
    globalDescription: isIt
      ? 'Analisi delle transazioni in tempo reale, controllo KYC e monitoraggio delle regole di conformità.'
      : 'Real-time transaction auditing, KYC checks, and compliance rules enforcement.',
    baseMetrics: { throughput: 3.1, latency: 2.0, cpu: 20, mem: 5.0 },
    anomalyNodeId: 'user-verification',
    warningAffects: ['limit-checks'],
    anomalyLogMessages: [
      isIt ? 'AVVISO: Controlli Utente: Limite della frequenza di elaborazione superato. Riprovo i payload.' : 'WARNING: User Verification: Processing rate limit exceeded. Retrying payloads.',
      isIt ? 'AVVISO: Soglia di latenza superata per la verifica del database.' : 'WARNING: Latency threshold exceeded for database verify.'
    ],
    links: [
      { from: 'financial-source', to: 'user-verification' },
      { from: 'financial-source', to: 'limit-checks' },
      { from: 'financial-source', to: 'pre-audit-sink' },
      { from: 'user-verification', to: 'decision-filter' },
      { from: 'limit-checks', to: 'decision-filter' },
      { from: 'decision-filter', to: 'notification-sink' },
      { from: 'decision-filter', to: 'post-audit-sink' }
    ],
    nodes: {
      'financial-source': {
        name: isIt ? 'Operazioni Bancarie' : 'Banking Transactions',
        type: isIt ? 'Sorgente' : 'Source',
        left: '6%', top: '50%',
        description: isIt
          ? 'Endpoint principale di ingestione delle transazioni bancarie. Legge flussi dalle code di messaggi, analizza i protocolli bancari ISO-8583 ed emette eventi di transazione normalizzati.'
          : 'Main banking transactions ingestion endpoint. Reads streams from message queues, parses ISO-8583 banking protocols, and emits normalized transaction events.',
        baseMetrics: { throughput: 6.2, latency: 0.1, cpu: 12, mem: 3.2 },
        customInfo: (metrics) => isIt
          ? `<strong>Ingestori Attivi:</strong> 4 worker<br><strong>Coda Buffer:</strong> 0.2% piena`
          : `<strong>Active Ingestors:</strong> 4 workers<br><strong>Buffer Queue:</strong> 0.2% full`,
        inputSample: `{\n  "raw_payload": "NjgxMiwxNTAuMDAsMTcxOTMyNDgwMCx1c3JfNDIwMQ==",\n  "channel": "ATM-Gateway-04",\n  "protocol": "ISO-8583"\n}`,
        outputSample: `{\n  "tx_id": "tx_2049182",\n  "user_id": "usr_4201",\n  "amount": 150.00,\n  "timestamp": "2026-06-26T10:45:00Z"\n}`
      },
      'user-verification': {
        name: isIt ? 'Controlli Utente' : 'User Verification',
        type: isIt ? 'Operatore' : 'Operator',
        left: '35%', top: '20%',
        description: isIt
          ? 'Esegue la verifica delle credenziali utente, controlli KYC e soglie di rischio.'
          : 'Performs verification of user credentials, KYC checks, and risk thresholds.',
        baseMetrics: { throughput: 5.1, latency: 1.8, cpu: 20, mem: 5.0 },
        customInfo: (metrics) => {
          if (simState.isWarningActive) {
            return isIt
              ? `<span style="color:var(--chart-4); font-weight:bold;">⚠️ Allerta: Limite di frequenza superato!</span><br><strong>Tasso di errore:</strong> 4.2%<br><strong>Avviso:</strong> Picco nei tempi di risposta.`
              : `<span style="color:var(--chart-4); font-weight:bold;">⚠️ Alert: Rate Limit exceeded!</span><br><strong>Failure rate:</strong> 4.2%<br><strong>Warning:</strong> Response times spiking.`;
          }
          return isIt
            ? `<strong>Controlli Attivi:</strong> 12 regole<br><strong>Cache Hit Rate:</strong> 94.2%`
            : `<strong>Active Checks:</strong> 12 policies<br><strong>Cache Hit Rate:</strong> 94.2%`;
        },
        customBubble: (metrics) => {
          if (simState.isWarningActive) {
            return isIt 
              ? `<div class="warning-alert-text">Warning: Limite Frequenza!</div>`
              : `<div class="warning-alert-text">Warning: Rate Limit Exceeded!</div>`;
          }
          return '';
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
      'limit-checks': {
        name: isIt ? 'Controlli Transazione' : 'Transaction Limits',
        type: isIt ? 'Operatore' : 'Operator',
        left: '35%', top: '50%',
        description: isIt
          ? 'Valuta i payload delle transazioni rispetto alle soglie di conformità, controlli dei limiti e limiti giornalieri storici.'
          : 'Evaluates transaction payloads for compliance thresholds, limit checks, and daily historical limit rules.',
        baseMetrics: { throughput: 4.9, latency: 2.2, cpu: 18, mem: 4.8 },
        customInfo: (metrics) => isIt
          ? `<strong>Motore Regole:</strong> Drools v8.1<br><strong>Latenza Valutazione:</strong> 1.5ms`
          : `<strong>Rule Engine:</strong> Drools v8.1<br><strong>Evaluation Latency:</strong> 1.5ms`,
        inputSample: `{\n  "tx_id": "tx_2049182",\n  "user_id": "usr_4201",\n  "amount": 150.00,\n  "timestamp": "2026-06-26T10:45:00Z"\n}`,
        outputSample: `{\n  "tx_id": "tx_2049182",\n  "user_id": "usr_4201",\n  "amount": 150.00,\n  "timestamp": "2026-06-26T10:45:00Z",\n  "compliance_status": "PASSED",\n  "limit_check": "OK"\n}`
      },
      'pre-audit-sink': {
        name: isIt ? 'Salvataggio Audit' : 'Audit Storage',
        type: isIt ? 'Destinazione' : 'Sink',
        left: '35%', top: '80%',
        description: isIt
          ? 'Persiste i payload grezzi delle transazioni pre-filtrate direttamente nei log di audit in cold storage per lo storico di sicurezza.'
          : 'Persists raw pre-filtered transaction payloads directly into cold storage audit logs for security history.',
        baseMetrics: { throughput: 1.1, latency: 12.5, cpu: 15, mem: 2.1 },
        customInfo: (metrics) => isIt
          ? `<strong>Target Storage:</strong> Backup S3 Glacier<br><strong>Stato Scrittura:</strong> Confermato`
          : `<strong>Storage Target:</strong> S3 Glacier Backup<br><strong>Write Status:</strong> Acknowledged`,
        inputSample: `{\n  "tx_id": "tx_2049182",\n  "user_id": "usr_4201",\n  "amount": 150.00,\n  "timestamp": "2026-06-26T10:45:00Z"\n}`,
        outputSample: `{\n  "db_status": "SUCCESS",\n  "inserted_rows": 1,\n  "table": "raw_audit_logs",\n  "hash": "8f3b23c21a4f00129bc90812"\n}`
      },
      'decision-filter': {
        name: isIt ? 'Accetta / Rifiuto' : 'Accept / Reject',
        type: isIt ? 'Filtro' : 'Filter',
        left: '65%', top: '50%',
        description: isIt
          ? 'Valuta le metriche di rischio del payload delle transazioni e lo stato di verifica per filtrare o instradare i payload.'
          : 'Evaluates transaction payload risk metrics and verification status to filter or route payloads.',
        baseMetrics: { throughput: 4.8, latency: 0.5, cpu: 22, mem: 5.1 },
        customInfo: (metrics) => {
          const acceptRate = simState.isWarningActive ? '88%' : '98%';
          const rejectRate = simState.isWarningActive ? '12%' : '2%';
          return isIt
            ? `<strong>Tassi Filtro:</strong><br><span style="color:var(--chart-2); font-weight:bold;">✔ Accetta: ${acceptRate}</span><br><span style="color:var(--chart-4); font-weight:bold;">✖ Rifiuto: ${rejectRate}</span>`
            : `<strong>Filter Rates:</strong><br><span style="color:var(--chart-2); font-weight:bold;">✔ Accept: ${acceptRate}</span><br><span style="color:var(--chart-4); font-weight:bold;">✖ Reject: ${rejectRate}</span>`;
        },
        customBubble: (metrics) => {
          const acceptPct = simState.isWarningActive ? 88 : 98;
          const rejectPct = simState.isWarningActive ? 12 : 2;
          return isIt 
            ? `% Filtrati: <span class="bubble-stat">${acceptPct}% Accetta, ${rejectPct}% Rifiuto</span>`
            : `% Filtered: <span class="bubble-stat">${acceptPct}% Accept, ${rejectPct}% Reject</span>`;
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
      'notification-sink': {
        name: isIt ? 'Invio Conferma' : 'Send Notification',
        type: isIt ? 'Destinazione' : 'Sink',
        left: '92%', top: '35%',
        description: isIt
          ? 'Invia notifiche agli utenti tramite SMS o Email per le transazioni andate a buon fine.'
          : 'Dispatches notifications to users via SMS or Email notifications for successful transactions.',
        baseMetrics: { throughput: 4.7, latency: 15.2, cpu: 10, mem: 1.8 },
        customInfo: (metrics) => isIt
          ? `<strong>SMTP Gateway:</strong> Connesso<br><strong>Dimensione Coda:</strong> 0 messaggi in attesa`
          : `<strong>SMTP Gateway:</strong> Connected<br><strong>Queue Size:</strong> 0 messages pending`,
        inputSample: `{\n  "tx_id": "tx_2049182",\n  "status": "APPROVED"\n}`,
        outputSample: `{\n  "notification_sent": true,\n  "channel": "SMS",\n  "to": "+39 333 420104",\n  "status": "DELIVERED"\n}`
      },
      'post-audit-sink': {
        name: isIt ? 'Salvataggio Audit' : 'Audit Log Sink',
        type: isIt ? 'Destinazione' : 'Sink',
        left: '92%', top: '65%',
        description: isIt
          ? 'Salva gli elementi del registro filtrati e approvati nelle tabelle del database PostgreSQL per la contabilità a valle.'
          : 'Saves filtered and approved ledger items into PostgreSQL db tables for downstream accounting.',
        baseMetrics: { throughput: 4.7, latency: 8.1, cpu: 11, mem: 2.3 },
        customInfo: (metrics) => isIt
          ? `<strong>DB Target:</strong> PostgreSQL Audit Cluster<br><strong>Connessioni Pool:</strong> 12/20 attive`
          : `<strong>DB Target:</strong> PostgreSQL Audit Cluster<br><strong>Pool Connections:</strong> 12/20 active`,
        inputSample: `{\n  "tx_id": "tx_2049182",\n  "status": "APPROVED"\n}`,
        outputSample: `{\n  "db_status": "SUCCESS",\n  "inserted_rows": 1,\n  "table": "processed_ledger",\n  "ledger_id": "L-908124"\n}`
      }
    }
  },
  iot: {
    name: isIt ? 'Telemetria IoT Fleet' : 'IoT Fleet Telemetry',
    globalDescription: isIt
      ? 'Monitoraggio sensori industriali, aggregazione in tempo reale tramite finestre temporali e rilevamento anomalie.'
      : 'Industrial sensor monitoring, real-time aggregation via tumbling windows, and anomaly filtering.',
    baseMetrics: { throughput: 14.5, latency: 1.2, cpu: 25, mem: 4.2 },
    anomalyNodeId: 'anomaly-detect',
    warningAffects: ['alert-dispatcher'],
    anomalyLogMessages: [
      isIt ? 'AVVISO: Filtro Anomalie: Rilevato picco di temperatura insolito su sensor_08b (82.4°C).' : 'WARNING: Anomaly Filter: Unusual temperature spike detected on sensor_08b (82.4C).',
      isIt ? 'AVVISO: Dispatcher Allarmi: Invio notifica di criticità manutenzione a PagerDuty.' : 'WARNING: Alert Dispatcher: Dispatched critical maintenance alert to PagerDuty.'
    ],
    links: [
      { from: 'iot-source', to: 'window-agg' },
      { from: 'iot-source', to: 'edge-storage' },
      { from: 'window-agg', to: 'anomaly-detect' },
      { from: 'anomaly-detect', to: 'alert-dispatcher' },
      { from: 'anomaly-detect', to: 'metrics-sink' }
    ],
    nodes: {
      'iot-source': {
        name: isIt ? 'Flusso Telemetria' : 'Telemetry Stream',
        type: isIt ? 'Sorgente' : 'Source',
        left: '6%', top: '50%',
        description: isIt
          ? 'Legge flussi MQTT/Kafka ad alta frequenza emessi da migliaia di sensori distribuiti su macchine e motori della fabbrica.'
          : 'Consumes high-frequency MQTT/Kafka telemetry streams emitted from thousands of sensors on factory machinery.',
        baseMetrics: { throughput: 28.4, latency: 0.05, cpu: 15, mem: 2.8 },
        customInfo: (metrics) => isIt
          ? `<strong>Sensori Attivi:</strong> 1,240 dispositivi<br><strong>Buffer Coda Edge:</strong> In salute`
          : `<strong>Active Sensors:</strong> 1,240 devices<br><strong>Edge Buffer:</strong> Healthy`,
        inputSample: `{\n  "sensor_id": "sensor_08b",\n  "metric": "temp_c",\n  "value": 82.4,\n  "ts": 1719324810\n}`,
        outputSample: `{\n  "sensor_id": "sensor_08b",\n  "type": "TEMP",\n  "reading": 82.4,\n  "timestamp": "2026-06-29T14:13:30Z"\n}`
      },
      'window-agg': {
        name: isIt ? 'Finestra Temporale' : 'Tumbling Window',
        type: isIt ? 'Operatore' : 'Operator',
        left: '35%', top: '30%',
        description: isIt
          ? 'Raggruppa le misurazioni dei sensori in finestre temporali di 10 secondi calcolando medie correnti e riducendo il rumore.'
          : 'Groups telemetry measurements into 10-second tumbling windows to compute rolling averages and reduce data noise.',
        baseMetrics: { throughput: 25.1, latency: 1.1, cpu: 22, mem: 4.8 },
        customInfo: (metrics) => isIt
          ? `<strong>Durata Finestra:</strong> 10s (Tumbling)<br><strong>Funzione di Riduzione:</strong> Media Statistica`
          : `<strong>Window Duration:</strong> 10s (Tumbling)<br><strong>Reduction Fn:</strong> Statistical Average`,
        inputSample: `{\n  "sensor_id": "sensor_08b",\n  "reading": 82.4,\n  "timestamp": "2026-06-29T14:13:30Z"\n}`,
        outputSample: `{\n  "window_start": 1719324800,\n  "window_end": 1719324810,\n  "sensor_id": "sensor_08b",\n  "avg_value": 81.9,\n  "max_value": 82.4,\n  "data_points": 120\n}`
      },
      'edge-storage': {
        name: isIt ? 'Cache Locale' : 'Local Cache',
        type: isIt ? 'Destinazione' : 'Sink',
        left: '35%', top: '70%',
        description: isIt
          ? 'Salva i flussi grezzi di telemetria ad alta risoluzione in database locali all\'Edge per scopi diagnostici e di ripristino rapido.'
          : 'Persists raw high-resolution telemetry locally on disk at the Edge for debugging and historical data replay.',
        baseMetrics: { throughput: 3.3, latency: 6.8, cpu: 8, mem: 1.9 },
        customInfo: (metrics) => isIt
          ? `<strong>Motore Local DB:</strong> SQLite v3.45<br><strong>Bitrate Scrittura:</strong> 2.4 MB/s`
          : `<strong>Local DB Engine:</strong> SQLite v3.45<br><strong>Write Bitrate:</strong> 2.4 MB/s`,
        inputSample: `{\n  "sensor_id": "sensor_08b",\n  "reading": 82.4\n}`,
        outputSample: `{\n  "write_status": "COMMITTED",\n  "bytes_written": 256,\n  "timestamp": 1719324810\n}`
      },
      'anomaly-detect': {
        name: isIt ? 'Filtro Anomalie' : 'Anomaly Filter',
        type: isIt ? 'Filtro' : 'Filter',
        left: '65%', top: '30%',
        description: isIt
          ? 'Confronta le medie calcolate nelle finestre con le soglie di sicurezza delle macchine per rilevare surriscaldamenti o vibrazioni insolite.'
          : 'Compares window averages against machinery safety envelopes to filter and trigger alerts for heat/vibrations spikes.',
        baseMetrics: { throughput: 24.8, latency: 0.4, cpu: 28, mem: 3.5 },
        customInfo: (metrics) => {
          if (simState.isWarningActive) {
            return isIt
              ? `<span style="color:var(--chart-4); font-weight:bold;">⚠️ Allerta: Anomalia Rilevata!</span><br><strong>Temperatura sensor_08b:</strong> 82.4°C > Soglia 80.0°C`
              : `<span style="color:var(--chart-4); font-weight:bold;">⚠️ Alert: Anomaly Triggered!</span><br><strong>sensor_08b Temp:</strong> 82.4C > Limit 80.0C`;
          }
          return isIt
            ? `<strong>Analisi Soglia:</strong> Attiva (Modello Deviazione Standard)<br><strong>Stato Allarmi:</strong> Nessuna anomalia`
            : `<strong>Sentry Check:</strong> Active (StdDev Dev Model)<br><strong>Alarm Status:</strong> Nominal`;
        },
        customBubble: (metrics) => {
          if (simState.isWarningActive) {
            return isIt 
              ? `<div class="warning-alert-text">Warning: Anomalia Rilevata!</div>`
              : `<div class="warning-alert-text">Warning: Anomaly Detected!</div>`;
          }
          return '';
        },
        inputSample: `{\n  "window_start": 1719324800,\n  "avg_value": 81.9,\n  "sensor_id": "sensor_08b"\n}`,
        outputSample: () => {
          if (simState.isWarningActive) {
            return JSON.stringify({
              "sensor_id": "sensor_08b",
              "trigger_metric": "temp_c",
              "severity": "CRITICAL",
              "avg_value": 81.9,
              "current_value": 82.4,
              "threshold": 80.0,
              "anomaly_detected": true
            }, null, 2);
          }
          return JSON.stringify({
            "sensor_id": "sensor_08b",
            "anomaly_detected": false
          }, null, 2);
        }
      },
      'alert-dispatcher': {
        name: isIt ? 'Avviso Emergenza' : 'PagerDuty Alert',
        type: isIt ? 'Destinazione' : 'Sink',
        left: '92%', top: '20%',
        description: isIt
          ? 'Invia avvisi ad alta priorità e apre ticket di manutenzione su piattaforme esterne quando viene confermata un\'anomalia.'
          : 'Dispatches instant priority incidents and maintenance alerts to PagerDuty or Slack channels on anomaly triggers.',
        baseMetrics: { throughput: 0.1, latency: 14.5, cpu: 5, mem: 1.2 },
        customInfo: (metrics) => isIt
          ? `<strong>Canale Uscita:</strong> PagerDuty webhook<br><strong>Stato Chiamate:</strong> Connesso`
          : `<strong>Incident Target:</strong> PagerDuty Webhook<br><strong>Channel Health:</strong> Connected`,
        inputSample: `{\n  "sensor_id": "sensor_08b",\n  "severity": "CRITICAL"\n}`,
        outputSample: () => {
          if (simState.isWarningActive) {
            return JSON.stringify({
              "incident_key": "incident-50912A",
              "pagerduty_status": "TRIGGERED",
              "assigned_team": "Maintenance-OnCall",
              "timestamp": "2026-06-29T14:13:35Z"
            }, null, 2);
          }
          return `{\n  "status": "IDLE",\n  "active_incidents": 0\n}`;
        }
      },
      'metrics-sink': {
        name: isIt ? 'Database Metriche' : 'Timescale Cloud',
        type: isIt ? 'Destinazione' : 'Sink',
        left: '92%', top: '50%',
        description: isIt
          ? 'Memorizza le letture aggregate delle finestre nel database cloud TimescaleDB per reportistiche storiche e dashboard analitiche.'
          : 'Stores aggregated telemetry metrics into TimescaleDB time-series database in the Cloud for dashboard analysis.',
        baseMetrics: { throughput: 24.7, latency: 8.5, cpu: 12, mem: 2.5 },
        customInfo: (metrics) => isIt
          ? `<strong>Target DB:</strong> TimescaleDB Cloud Cluster<br><strong>Ritardo Ingestione:</strong> 8.2ms`
          : `<strong>Target Database:</strong> TimescaleDB Cloud<br><strong>Ingestion Delay:</strong> 8.2ms`,
        inputSample: `{\n  "sensor_id": "sensor_08b",\n  "avg_value": 81.9\n}`,
        outputSample: `{\n  "db_write": "SUCCESS",\n  "records_inserted": 1,\n  "table": "sensor_averages_10s"\n}`
      }
    }
  },
  cdn: {
    name: isIt ? 'Analisi Web CDN' : 'CDN Web Analytics',
    globalDescription: isIt
      ? 'Elaborazione log web in tempo reale, arricchimento Geo-IP, filtraggio crawler/bot malevoli e calcolo sessioni utente.'
      : 'Real-time CDN web logs processing, Geo-IP lookup enrichment, malicious bot filtering, and user sessionization.',
    baseMetrics: { throughput: 18.2, latency: 0.8, cpu: 28, mem: 5.6 },
    anomalyNodeId: 'bot-detector',
    warningAffects: ['sessionizer'],
    anomalyLogMessages: [
      isIt ? 'AVVISO: Filtro Bot: Rilevata firma di attacco DDoS DDoS-Scraper dall\'IP 192.168.1.100. Richieste bloccate.' : 'WARNING: Bot Filter: Malicious DDoS-Scraper signature detected from IP 192.168.1.100. Requests blocked.',
      isIt ? 'AVVISO: Localizzazione IP: Rilevato traffico insolitamente alto per la sotto-rete IP 192.168.1.0/24.' : 'WARNING: Geo-IP Lookup: Unusually high traffic detected for subnetwork 192.168.1.0/24.'
    ],
    links: [
      { from: 'cdn-source', to: 'geo-lookup' },
      { from: 'cdn-source', to: 'bot-detector' },
      { from: 'geo-lookup', to: 'sessionizer' },
      { from: 'bot-detector', to: 'sessionizer' },
      { from: 'sessionizer', to: 'clickstream-sink' },
      { from: 'sessionizer', to: 'realtime-dashboard' }
    ],
    nodes: {
      'cdn-source': {
        name: isIt ? 'Log Web CDN' : 'CDN Web Logs',
        type: isIt ? 'Sorgente' : 'Source',
        left: '6%', top: '50%',
        description: isIt
          ? 'Endpoint di acquisizione che aggrega i log di accesso in tempo reale da tutti i server CDN distribuiti nel mondo.'
          : 'High-volume log collector aggregating real-time access log packets from edge CDN servers globally.',
        baseMetrics: { throughput: 42.1, latency: 0.05, cpu: 18, mem: 3.5 },
        customInfo: (metrics) => isIt
          ? `<strong>Server Ingest:</strong> 15 Edge CDN Nodes<br><strong>Log Ingest Rate:</strong> 42.1k reqs/s`
          : `<strong>Active Nodes:</strong> 15 Edge CDN Nodes<br><strong>Log Ingest Rate:</strong> 42.1k reqs/s`,
        inputSample: `{\n  "ip": "192.168.1.100",\n  "agent": "Mozilla/5.0",\n  "uri": "/index.html",\n  "bytes": 4510\n}`,
        outputSample: `{\n  "request_id": "req_8019A",\n  "ip_address": "192.168.1.100",\n  "user_agent": "Mozilla/5.0",\n  "uri": "/index.html"\n}`
      },
      'geo-lookup': {
        name: isIt ? 'Localizzazione IP' : 'Geo-IP Lookup',
        type: isIt ? 'Operatore' : 'Operator',
        left: '35%', top: '20%',
        description: isIt
          ? 'Interroga un database GeoIP MaxMind residente in memoria ad alte prestazioni per arricchire il record di log con paese, città e ISP.'
          : 'Enriches raw log entries with geographical details (country, region, city) using an ultra-fast in-memory lookup cache.',
        baseMetrics: { throughput: 38.5, latency: 0.3, cpu: 25, mem: 6.8 },
        customInfo: (metrics) => isIt
          ? `<strong>Cache DB:</strong> MaxMind GeoIP2 Lite<br><strong>Hit Rate:</strong> 99.8%`
          : `<strong>Database Cache:</strong> MaxMind GeoIP2 Lite<br><strong>Hit Rate:</strong> 99.8%`,
        inputSample: `{\n  "ip_address": "192.168.1.100"\n}`,
        outputSample: `{\n  "ip_address": "192.168.1.100",\n  "geo": {\n    "country": "US",\n    "city": "Chicago",\n    "lat": 41.87,\n    "lon": -87.62\n  }\n}`
      },
      'bot-detector': {
        name: isIt ? 'Filtro Bot' : 'Bot Filter',
        type: isIt ? 'Filtro' : 'Filter',
        left: '35%', top: '50%',
        description: isIt
          ? 'Analizza le firme delle richieste, i tassi di click e le soglie insolite per rilevare ed eliminare crawlers di motori di ricerca, scrapers e bot DDoS.'
          : 'Inspects requests fingerprints and access frequencies to identify and filter malicious scrapers, scrapers, and DDoS networks.',
        baseMetrics: { throughput: 38.2, latency: 0.6, cpu: 32, mem: 4.8 },
        customInfo: (metrics) => {
          if (simState.isWarningActive) {
            return isIt
              ? `<span style="color:var(--chart-4); font-weight:bold;">⚠️ DDoS Rilevato!</span><br><strong>Attacchi bloccati:</strong> 1,480 req/s<br><strong>Tasso Blocco:</strong> 98.4%`
              : `<span style="color:var(--chart-4); font-weight:bold;">⚠️ DDoS Detected!</span><br><strong>Blocked requests:</strong> 1,480 req/s<br><strong>Drop Rate:</strong> 98.4%`;
          }
          return isIt
            ? `<strong>Regole Bot:</strong> 150 firme attive<br><strong>Crawler Riconosciuti:</strong> Googlebot, Bingbot`
            : `<strong>Bot Database:</strong> 150 active signatures<br><strong>Recognized Bots:</strong> Googlebot, Bingbot`;
        },
        customBubble: (metrics) => {
          if (simState.isWarningActive) {
            return isIt 
              ? `<div class="warning-alert-text">Warning: DDoS Filtro Attivo!</div>`
              : `<div class="warning-alert-text">Warning: DDoS Filter Active!</div>`;
          }
          return '';
        },
        inputSample: `{\n  "ip_address": "192.168.1.100",\n  "user_agent": "Mozilla/5.0"\n}`,
        outputSample: () => {
          if (simState.isWarningActive) {
            return JSON.stringify({
              "ip_address": "192.168.1.100",
              "action": "BLOCK_REQUEST",
              "reason": "DDOS_SCRAPER_SIGNATURE",
              "risk_score": 1.00
            }, null, 2);
          }
          return JSON.stringify({
            "ip_address": "192.168.1.100",
            "action": "ALLOW_REQUEST",
            "risk_score": 0.01
          }, null, 2);
        }
      },
      'sessionizer': {
        name: isIt ? 'Finestra Sessione' : 'Session Window',
        type: isIt ? 'Operatore' : 'Operator',
        left: '65%', top: '35%',
        description: isIt
          ? 'Costruisce sessioni di navigazione utente raggruppando le richieste basandosi su cookie o IP, con un timeout di inattività di 30 minuti.'
          : 'Groups web requests into user navigation sessions based on session tokens or IP addresses, using a 30-minute inactivity timeout.',
        baseMetrics: { throughput: 36.8, latency: 1.5, cpu: 26, mem: 7.2 },
        customInfo: (metrics) => isIt
          ? `<strong>Tipo Finestra:</strong> Finestra di Sessione (Timeout 30m)<br><strong>Sessioni Attive:</strong> 14,280 in RAM`
          : `<strong>Window Mode:</strong> Session Gap (30m timeout)<br><strong>Active Sessions:</strong> 14,280 stateful`,
        inputSample: `{\n  "request_id": "req_8019A",\n  "ip_address": "192.168.1.100",\n  "geo": {"country": "US"}\n}`,
        outputSample: `{\n  "session_id": "sess_us_chicago_908",\n  "clicks": 4,\n  "duration_seconds": 182,\n  "landing_page": "/index.html",\n  "is_completed": false\n}`
      },
      'clickstream-sink': {
        name: isIt ? 'Store Navigazione' : 'Clickstream Store',
        type: isIt ? 'Destinazione' : 'Sink',
        left: '92%', top: '20%',
        description: isIt
          ? 'Salva le sessioni utente chiuse in Apache Iceberg o Parquet su S3 per reportistiche mensili e addestramento modelli AI.'
          : 'Saves closed user session packets into Apache Iceberg or S3 Parquet formats for long-term offline behavior analytics.',
        baseMetrics: { throughput: 8.2, latency: 12.0, cpu: 14, mem: 2.8 },
        customInfo: (metrics) => isIt
          ? `<strong>Format:</strong> Apache Parquet / S3<br><strong>Compattazione:</strong> Attiva`
          : `<strong>Storage Format:</strong> Apache Parquet / S3<br><strong>Compacting:</strong> Active`,
        inputSample: `{\n  "session_id": "sess_us_chicago_908",\n  "clicks": 4,\n  "duration_seconds": 182\n}`,
        outputSample: `{\n  "write_status": "COMMITTED",\n  "file_path": "s3://clickstream/2026/06/29/sess_908.parquet"\n}`
      },
      'realtime-dashboard': {
        name: isIt ? 'Metriche Live Redis' : 'Redis Live Stats',
        type: isIt ? 'Destinazione' : 'Sink',
        left: '92%', top: '50%',
        description: isIt
          ? 'Aggiorna contatori in tempo reale, statistiche geografiche dei visitatori e metriche attive visualizzate in RAM per le dashboard aziendali.'
          : 'Increments real-time click counters, country rankings, and active user stats in a high-performance Redis database.',
        baseMetrics: { throughput: 36.8, latency: 1.1, cpu: 15, mem: 2.1 },
        customInfo: (metrics) => isIt
          ? `<strong>Redis Host:</strong> cluster-live.redis.internal<br><strong>Latenza Scrittura:</strong> 0.4ms`
          : `<strong>Redis Target:</strong> cluster-live.redis.internal<br><strong>Write Latency:</strong> 0.4ms`,
        inputSample: `{\n  "session_id": "sess_us_chicago_908",\n  "clicks": 4\n}`,
        outputSample: `{\n  "redis_status": "SUCCESS",\n  "key_updated": "live:clicks:us",\n  "ttl_refresh_sec": 1800\n}`
      }
    }
  }
};

// Select and load a pipeline scenario configuration
function selectScenario(scenarioId) {
  if (!scenarios[scenarioId]) return;

  simState.currentScenario = scenarioId;
  nodesConfig = scenarios[scenarioId].nodes;

  // Rebuild global configs
  globalPipelineConfig = {
    name: scenarios[scenarioId].name,
    type: isIt ? 'Globale' : 'Global',
    description: scenarios[scenarioId].globalDescription,
    baseMetrics: scenarios[scenarioId].baseMetrics,
    customInfo: (metrics) => {
      const statusText = simState.isWarningActive 
        ? (isIt ? 'ANOMALIA' : 'WARNING') 
        : (isIt ? 'FUNZIONANTE' : 'HEALTHY');
      return isIt
        ? `<strong>Operatori Attivi:</strong> ${Object.keys(nodesConfig).length} nodi<br><strong>Stato Pipeline:</strong> ${statusText}`
        : `<strong>Active Operators:</strong> ${Object.keys(nodesConfig).length} nodes<br><strong>Pipeline Status:</strong> ${statusText}`;
    }
  };

  // Reinitialize Metrics values & Sparkline Histories
  initMetrics();

  // Reset viewport state
  canvasState.panX = 0;
  canvasState.panY = 0;
  canvasState.scale = 1.0;
  const viewport = document.getElementById('dag-viewport');
  if (viewport) {
    viewport.style.transform = `translate(0px, 0px) scale(1.0)`;
  }

  // Clear node selection
  simState.selectedNode = null;

  // Re-generate HTML nodes in viewport
  renderNodes();

  // Re-calculate & draw connection pathways
  drawConnections();

  // Apply warning triggers (clears them)
  simState.isWarningActive = false;
  applyWarningState();

  // Update inspection details card
  updateInspectorPanel();

  logToConsole(isIt 
    ? `Caricato scenario demo: ${scenarios[scenarioId].name}` 
    : `Loaded scenario demo: ${scenarios[scenarioId].name}`);
}

// Dynamically generate node elements in the DOM
function renderNodes() {
  const viewport = document.getElementById('dag-viewport');
  if (!viewport) return;

  // Clear existing nodes from the DAG container
  viewport.querySelectorAll('.dag-node').forEach(node => node.remove());

  // Loop configs and create nodes
  Object.keys(nodesConfig).forEach(id => {
    const node = nodesConfig[id];

    const nodeEl = document.createElement('div');
    nodeEl.className = 'dag-node absolute';
    nodeEl.id = `node-${id}`;
    nodeEl.setAttribute('data-node-id', id);
    nodeEl.style.left = node.left;
    nodeEl.style.top = node.top;

    nodeEl.innerHTML = `
      <div class="node-border"></div>
      <div class="node-content">
        <span class="node-type">${node.type}</span>
        <span class="node-title">${node.name}</span>
      </div>
      <div class="node-sketch-bubble">
        <div class="bubble-border"></div>
        <div class="bubble-content">
          <div>throughput: <span class="bubble-stat" id="bubble-stat-${id}-throughput">...</span></div>
          <div>CPU: <span class="bubble-stat" id="bubble-stat-${id}-cpu">...</span></div>
          <div>Mem: <span class="bubble-stat" id="bubble-stat-${id}-mem">...</span></div>
          <div id="bubble-stat-${id}-custom" class="bubble-custom-field"></div>
        </div>
      </div>
    `;

    // Direct click selection listener
    nodeEl.addEventListener('click', (e) => {
      e.stopPropagation();
      selectNode(id);
    });

    viewport.appendChild(nodeEl);
  });
}

// Select a node and toggle states
function selectNode(nodeId) {
  const nodeEl = document.getElementById(`node-${nodeId}`);
  if (!nodeEl) return;

  if (simState.selectedNode === nodeId) {
    deselectAllNodes();
  } else {
    document.querySelectorAll('.dag-node').forEach(n => n.classList.remove('selected'));
    nodeEl.classList.add('selected');
    simState.selectedNode = nodeId;

    logToConsole(isIt 
      ? `Operatore selezionato: ${nodesConfig[nodeId].name}` 
      : `Selected node: ${nodesConfig[nodeId].name}`);
    
    updateInspectorPanel();
    drawConnections();
  }
}

// Deselect selected node and return to overall pipeline metrics
function deselectAllNodes() {
  if (simState.selectedNode === null) return;

  document.querySelectorAll('.dag-node').forEach(n => n.classList.remove('selected'));
  simState.selectedNode = null;

  logToConsole(isIt 
    ? 'Reset selezione: visualizzazione dell\'intera topologia della pipeline.' 
    : 'Reset selection: viewing entire pipeline topology.');
  
  updateInspectorPanel();
  drawConnections();
}

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

  const activeScenario = scenarios[simState.currentScenario];

  // 1. Update nodes
  Object.keys(nodesConfig).forEach(id => {
    const config = nodesConfig[id];
    const metrics = metricsData[id];
    if (!metrics) return;

    // Base fluctuation
    let devT = (Math.random() - 0.5) * 0.1;
    let devL = (Math.random() - 0.5) * 0.3;
    let devC = (Math.random() - 0.5) * 1.0;
    
    // Apply simulation scale
    metrics.throughput = Math.max(0.1, config.baseMetrics.throughput * simState.speedScale + devT);

    // Apply scenario anomalies
    const isAnomalousNode = id === activeScenario.anomalyNodeId;
    const isAffectedNode = activeScenario.warningAffects && activeScenario.warningAffects.includes(id);

    if (simState.isWarningActive && (isAnomalousNode || isAffectedNode)) {
      // Spikes in CPU and Latency, drop in throughput
      metrics.throughput *= 0.6;
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
  if (globalMetrics) {
    let devT = (Math.random() - 0.5) * 0.05;
    let devL = (Math.random() - 0.5) * 0.1;
    let devC = (Math.random() - 0.5) * 0.5;

    globalMetrics.throughput = Math.max(0.1, globalPipelineConfig.baseMetrics.throughput * simState.speedScale + devT);
    
    if (simState.isWarningActive) {
      globalMetrics.latency = globalPipelineConfig.baseMetrics.latency * 2.0 + devL;
      globalMetrics.cpu = Math.min(95, globalPipelineConfig.baseMetrics.cpu * 2.2 + devC);
      // Add event warning occasionally
      if (Math.random() > 0.75 && activeScenario.anomalyLogMessages) {
        const msgs = activeScenario.anomalyLogMessages;
        const randomMsg = msgs[Math.floor(Math.random() * msgs.length)];
        logToConsole(randomMsg, 'warn');
      }
    } else {
      globalMetrics.latency = Math.max(0.1, globalPipelineConfig.baseMetrics.latency + devL);
      globalMetrics.cpu = Math.max(2, Math.min(98, globalPipelineConfig.baseMetrics.cpu + devC));
    }
    globalMetrics.mem = Math.max(1, globalPipelineConfig.baseMetrics.mem + (Math.random() - 0.5) * 0.02);

    // Save global history
    historyData['global'].push(globalMetrics.throughput);
    if (historyData['global'].length > simState.historyLength) historyData['global'].shift();
  }

  // 3. Update active UI elements
  updateInspectorPanel();
  updateFloatingBubbles();
}

// Draw connection curves using SVG
function drawConnections() {
  const svg = document.getElementById('connections-svg');
  const viewport = document.getElementById('dag-viewport');
  if (!svg || !viewport) return;

  const viewportRect = viewport.getBoundingClientRect();
  const scale = canvasState.scale;
  
  // Clear connections and paths
  const paths = svg.querySelectorAll('.connection-path, .particle-group');
  paths.forEach(p => p.remove());

  // Load active scenario links
  const links = scenarios[simState.currentScenario].links;

  links.forEach(link => {
    const fromEl = document.getElementById(`node-${link.from}`);
    const toEl = document.getElementById(`node-${link.to}`);
    if (!fromEl || !toEl) return;

    const fromRect = fromEl.getBoundingClientRect();
    const toRect = toEl.getBoundingClientRect();

    // From node center-right
    const x1 = (fromRect.right - viewportRect.left) / scale;
    const y1 = (fromRect.top + fromRect.height / 2 - viewportRect.top) / scale;

    // To node center-left
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
    
    // Select marker style
    const markerId = isActive ? 'arrow-green' : 'arrow-black';
    path.setAttribute('marker-end', `url(#${markerId})`);

    svg.appendChild(path);

    // Spawn animated floating bubbles (particles) along the path
    if (simState.isRunning) {
      const duration = (2.2 / simState.speedScale).toFixed(1) + 's';
      
      for (let i = 0; i < 2; i++) {
        const delay = ((i * 1.1) / simState.speedScale).toFixed(1) + 's';

        const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        g.setAttribute('class', 'particle-group');

        // Main bubble circle
        const bubble = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        bubble.setAttribute('r', '5.5');
        bubble.setAttribute('class', 'particle-bubble');
        if (isActive) {
          bubble.style.stroke = 'var(--chart-2)';
          bubble.style.fill = '#ebfbee';
        }
        g.appendChild(bubble);

        // Shiny dot for shading
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

// Draw sketchy SVG sparkline chart
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
    const y = height - padding - ((val - min) / range) * (height - padding * 2);

    const wiggleX = (Math.random() - 0.5) * 0.8;
    const wiggleY = (Math.random() - 0.5) * 0.8;

    if (i === 0) {
      pathD += `M ${x + wiggleX} ${y + wiggleY}`;
    } else {
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
  const activeScenario = scenarios[simState.currentScenario];
  if (simState.selectedNode && simState.selectedNode === activeScenario.anomalyNodeId && simState.isWarningActive) {
    pathEl.setAttribute('stroke', 'var(--chart-4)'); // red warning
  } else {
    pathEl.setAttribute('stroke', 'var(--chart-2)'); // green healthy
  }
}

// Update floating bubble statistics shown inside the DAG canvas
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

    const customEl = document.getElementById(`bubble-stat-${id}-custom`);
    if (customEl && nodesConfig[id].customBubble) {
      customEl.innerHTML = nodesConfig[id].customBubble(metrics);
    }
  });
}

// Update inspector notepad panel
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
  const activeScenario = scenarios[simState.currentScenario];

  if (selected && nodesConfig[selected]) {
    const config = nodesConfig[selected];
    const metrics = metricsData[selected];
    if (!metrics) return;

    titleEl.textContent = config.name;
    badgeEl.textContent = config.type;
    badgeEl.className = 'notebook-badge node-badge';

    // Status warning checks
    if (selected === activeScenario.anomalyNodeId && simState.isWarningActive) {
      statusEl.innerHTML = isIt 
        ? '<span style="color:var(--chart-4); font-weight:bold;">⚠️ Anomalia</span>'
        : '<span style="color:var(--chart-4); font-weight:bold;">⚠️ Warning</span>';
      badgeEl.className = 'notebook-badge warning-badge';
    } else {
      statusEl.innerHTML = isIt
        ? '<span style="color:var(--chart-2); font-weight:bold;">✔ Ottimale</span>'
        : '<span style="color:var(--chart-2); font-weight:bold;">✔ Healthy</span>';
    }

    throughputEl.textContent = `${metrics.throughput.toFixed(2)} M/s`;
    latencyEl.textContent = `${metrics.latency.toFixed(1)} ms`;
    cpuEl.textContent = `${Math.round(metrics.cpu)}%`;
    memEl.textContent = `${metrics.mem.toFixed(2)} Gb`;

    customSectionEl.querySelector('h3').textContent = isIt ? 'Configurazione Operatore' : 'Operator Configuration';
    
    const infoContent = config.customInfo ? config.customInfo(metrics) : '';
    customTextEl.innerHTML = `<p>${config.description}</p><div style="margin-top:10px;">${infoContent}</div>`;

    drawSparkline(historyData[selected]);

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
    // Global Pipeline
    const metrics = metricsData['global'];
    if (!metrics) return;

    titleEl.textContent = globalPipelineConfig.name;
    badgeEl.textContent = globalPipelineConfig.type;
    badgeEl.className = 'notebook-badge';

    if (simState.isWarningActive) {
      statusEl.innerHTML = isIt
        ? '<span style="color:var(--chart-4); font-weight:bold;">⚠️ Degradato</span>'
        : '<span style="color:var(--chart-4); font-weight:bold;">⚠️ Degraded</span>';
      badgeEl.className = 'notebook-badge warning-badge';
    } else {
      statusEl.innerHTML = isIt
        ? '<span style="color:var(--chart-2); font-weight:bold;">✔ Ottimale</span>'
        : '<span style="color:var(--chart-2); font-weight:bold;">✔ Healthy</span>';
    }

    throughputEl.textContent = `${metrics.throughput.toFixed(2)} M/s`;
    latencyEl.textContent = `${metrics.latency.toFixed(1)} ms`;
    cpuEl.textContent = `${Math.round(metrics.cpu)}%`;
    memEl.textContent = `${metrics.mem.toFixed(2)} Gb`;

    customSectionEl.querySelector('h3').textContent = isIt ? 'Info Topologia' : 'Topology Info';
    
    const infoContent = globalPipelineConfig.customInfo ? globalPipelineConfig.customInfo(metrics) : '';
    customTextEl.innerHTML = `<p>${globalPipelineConfig.description}</p><div style="margin-top:10px;">${infoContent}</div>`;

    drawSparkline(historyData['global']);

    const explainCard = document.getElementById('explainability-card');
    if (explainCard) {
      explainCard.classList.add('hidden');
    }
  }
}

// Log message to notepad console
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

  while (consoleEl.children.length > 35) {
    consoleEl.removeChild(consoleEl.firstChild);
  }
}

// Apply warning state triggers
function applyWarningState() {
  const activeScenario = scenarios[simState.currentScenario];
  const anomalyNode = document.getElementById(`node-${activeScenario.anomalyNodeId}`);
  const btnInjectWarning = document.getElementById('btn-inject-warning');
  const globalIndicator = document.getElementById('global-status-indicator');

  if (simState.isWarningActive) {
    if (btnInjectWarning) {
      btnInjectWarning.innerHTML = isIt ? '<span class="icon">✨</span> Risolvi Anomalia' : '<span class="icon">✨</span> Resolve Warning';
      btnInjectWarning.classList.add('active');
      btnInjectWarning.style.borderColor = 'var(--chart-2)';
      btnInjectWarning.style.color = 'var(--chart-2)';
      btnInjectWarning.style.boxShadow = '3px 3px 0px var(--chart-2)';
    }

    if (globalIndicator) {
      globalIndicator.className = 'status-indicator-box warning';
      globalIndicator.querySelector('.status-label').textContent = isIt ? 'Pipeline: ANOMALIA' : 'Pipeline: WARNING';
    }

    // Add warning highlight class on dynamic node elements
    if (anomalyNode) {
      anomalyNode.classList.add('warning');
    }

    // Log warning logs
    const msgs = activeScenario.anomalyLogMessages || [];
    msgs.forEach(msg => logToConsole(msg, 'warn'));
  } else {
    if (btnInjectWarning) {
      btnInjectWarning.innerHTML = isIt ? '<span class="icon">⚠️</span> Inietta Anomalia' : '<span class="icon">⚠️</span> Inject Warning';
      btnInjectWarning.classList.remove('active');
      btnInjectWarning.style.borderColor = '';
      btnInjectWarning.style.color = '';
      btnInjectWarning.style.boxShadow = '';
    }

    if (globalIndicator) {
      globalIndicator.className = 'status-indicator-box healthy';
      globalIndicator.querySelector('.status-label').textContent = isIt ? 'Pipeline: FUNZIONANTE' : 'Pipeline: HEALTHY';
    }

    document.querySelectorAll('.dag-node').forEach(node => {
      node.classList.remove('warning');
    });

    logToConsole(isIt 
      ? 'Allarmi di simulazione risolti. Ripristino del sistema...' 
      : 'Simulation alerts resolved. System recovering...');
  }

  updateInspectorPanel();
  drawConnections();
}

// Setup dropdown, speed slider, and warning toggles
function setupControls() {
  // Dropdown controls
  const dropdownToggle = document.getElementById('btn-dropdown-toggle');
  const dropdownMenu = document.getElementById('controls-dropdown-menu');
  const dropdownArrow = document.getElementById('dropdown-arrow');
  
  if (dropdownToggle && dropdownMenu) {
    dropdownToggle.addEventListener('click', (e) => {
      e.stopPropagation();
      const isHidden = dropdownMenu.classList.contains('hidden');
      if (isHidden) {
        dropdownMenu.classList.remove('hidden');
        dropdownToggle.setAttribute('aria-expanded', 'true');
        if (dropdownArrow) dropdownArrow.style.transform = 'rotate(180deg)';
      } else {
        dropdownMenu.classList.add('hidden');
        dropdownToggle.setAttribute('aria-expanded', 'false');
        if (dropdownArrow) dropdownArrow.style.transform = '';
      }
    });

    document.addEventListener('click', (e) => {
      if (!dropdownToggle.contains(e.target) && !dropdownMenu.contains(e.target)) {
        dropdownMenu.classList.add('hidden');
        dropdownToggle.setAttribute('aria-expanded', 'false');
        if (dropdownArrow) dropdownArrow.style.transform = '';
      }
    });
  }

  // Play Pause Toggle
  const btnPlayPause = document.getElementById('btn-play-pause');
  if (btnPlayPause) {
    btnPlayPause.addEventListener('click', () => {
      simState.isRunning = !simState.isRunning;
      
      if (simState.isRunning) {
        btnPlayPause.classList.add('active');
        btnPlayPause.innerHTML = isIt ? '<span class="icon">⏸</span> Pausa Sim' : '<span class="icon">⏸</span> Pause Sim';
        logToConsole(isIt ? 'Simulazione ripresa.' : 'Simulation resumed.');
        drawConnections();
      } else {
        btnPlayPause.classList.remove('active');
        btnPlayPause.innerHTML = isIt ? '<span class="icon">▶</span> Avvia Sim' : '<span class="icon">▶</span> Resume Sim';
        logToConsole(isIt ? 'Simulazione in pausa.' : 'Simulation paused.');
        drawConnections();
      }
    });
  }

  // Warning Button Toggle
  const btnInjectWarning = document.getElementById('btn-inject-warning');
  if (btnInjectWarning) {
    btnInjectWarning.addEventListener('click', () => {
      simState.isWarningActive = !simState.isWarningActive;
      applyWarningState();
    });
  }

  // Reset Selection
  const btnReset = document.getElementById('btn-clear-selection');
  if (btnReset) {
    btnReset.addEventListener('click', () => {
      deselectAllNodes();
    });
  }

  // Simulation Speed Slider
  const speedSlider = document.getElementById('simulation-speed');
  const speedVal = document.getElementById('speed-value');
  if (speedSlider) {
    speedSlider.addEventListener('input', (e) => {
      const val = parseFloat(e.target.value);
      simState.speedScale = val;
      if (speedVal) speedVal.textContent = `${val.toFixed(1)}x`;
      drawConnections();
    });
  }

  // Scenario Buttons Selection
  const scenarioBtns = document.querySelectorAll('.scenario-btn');
  scenarioBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      scenarioBtns.forEach(b => {
        b.classList.remove('active');
      });
      btn.classList.add('active');
      const scenarioId = btn.getAttribute('data-scenario');
      selectScenario(scenarioId);
    });
  });
}

// Window resizing adjustments
function setupWindowResize() {
  let resizeTimeout;
  window.addEventListener('resize', () => {
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(() => {
      drawConnections();
    }, 150);
  });
}

// Canvas Panning and Zooming Interaction
function setupCanvasPanning() {
  const container = document.getElementById('dag-container');
  const viewport = document.getElementById('dag-viewport');
  if (!container || !viewport) return;
  
  let isPanning = false;
  let startX = 0;
  let startY = 0;
  
  container.addEventListener('mousedown', (e) => {
    if (e.target.closest('.dag-node') || e.target.closest('button') || e.target.closest('input')) {
      return;
    }
    
    isPanning = true;
    container.style.cursor = 'grabbing';
    startX = e.clientX - canvasState.panX;
    startY = e.clientY - canvasState.panY;
    e.preventDefault();
  });
  
  window.addEventListener('mousemove', (e) => {
    if (!isPanning) return;
    
    canvasState.panX = e.clientX - startX;
    canvasState.panY = e.clientY - startY;
    
    updateTransform();
  });
  
  window.addEventListener('mouseup', () => {
    if (isPanning) {
      isPanning = false;
      container.style.cursor = 'grab';
    }
  });
  
  container.addEventListener('wheel', (e) => {
    e.preventDefault();
    const zoomIntensity = 0.05;
    
    if (e.deltaY < 0) {
      canvasState.scale = Math.min(2.0, canvasState.scale + zoomIntensity);
    } else {
      canvasState.scale = Math.max(0.5, canvasState.scale - zoomIntensity);
    }
    
    updateTransform();
    drawConnections();
  }, { passive: false });
  
  container.addEventListener('dblclick', (e) => {
    if (e.target.closest('.dag-node')) return;
    canvasState.panX = 0;
    canvasState.panY = 0;
    canvasState.scale = 1.0;
    updateTransform();
    drawConnections();
    logToConsole(isIt ? 'Reset dell\'inquadratura.' : 'Reset canvas viewport.');
  });
  
  function updateTransform() {
    viewport.style.transform = `translate(${canvasState.panX}px, ${canvasState.panY}px) scale(${canvasState.scale})`;
  }
}

// Initial Bootstrap
function init() {
  selectScenario('financial');
  
  // Set background click deselect
  const container = document.getElementById('dag-container');
  if (container) {
    container.addEventListener('click', (e) => {
      // If clicking exactly the canvas viewport background or connections SVG
      if (e.target === container || e.target.id === 'dag-viewport' || e.target.id === 'connections-svg') {
        deselectAllNodes();
      }
    });
  }

  setupControls();
  setupCanvasPanning();
  setupWindowResize();

  setTimeout(() => {
    drawConnections();
  }, 100);

  simState.tickInterval = setInterval(() => {
    updateMetrics();
  }, 1000);

  logToConsole(isIt 
    ? 'Benvenuto nel monitor di pipeline Renoir!' 
    : 'Welcome to the Renoir pipeline monitor mockup!');
}

document.addEventListener('DOMContentLoaded', init);
