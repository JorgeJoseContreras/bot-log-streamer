import os
from typing import List, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Header, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI(title="Jorge's Bot Log Streamer")

# Simple API key for security
API_KEY = os.getenv("LOG_STREAMER_API_KEY", "super-secret-key")

# File persistence for logs
LOG_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server_logs.txt")

# Load existing log file into memory on startup
GLOBAL_LOG_HISTORY: List[str] = []
if os.path.exists(LOG_FILE_PATH):
    try:
        with open(LOG_FILE_PATH, "r", encoding="utf-8") as f:
            GLOBAL_LOG_HISTORY = [line.rstrip("\r\n") for line in f.readlines() if line.strip()]
    except Exception:
        GLOBAL_LOG_HISTORY = []

MAX_HISTORY_LINES = 50000

# Global Bot State
current_bot_state = {
    "status": "idle",
    "detail": "Ready for commands",
    "tool": ""
}

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        # Send initial full log history and current bot status to newly connected client
        await websocket.send_json({
            "type": "init",
            "logs": GLOBAL_LOG_HISTORY[-2000:],
            "state": current_bot_state
        })

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

class LogPayload(BaseModel):
    logs: List[str]

class StatusPayload(BaseModel):
    status: str
    detail: Optional[str] = ""
    tool: Optional[str] = ""

@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    return HTML_CONTENT

@app.get("/api/logs/history")
async def get_log_history():
    return {"total_lines": len(GLOBAL_LOG_HISTORY), "logs": GLOBAL_LOG_HISTORY[-5000:]}

@app.post("/api/logs")
async def receive_logs(payload: LogPayload, x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    
    global GLOBAL_LOG_HISTORY
    GLOBAL_LOG_HISTORY.extend(payload.logs)
    if len(GLOBAL_LOG_HISTORY) > MAX_HISTORY_LINES:
        GLOBAL_LOG_HISTORY = GLOBAL_LOG_HISTORY[-MAX_HISTORY_LINES:]
        
    # Append to local server_logs.txt file
    try:
        with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
            for log_line in payload.logs:
                f.write(log_line + "\n")
    except Exception:
        pass
        
    await manager.broadcast({"type": "logs", "data": payload.logs})
    return {"status": "success", "broadcasted": len(payload.logs)}

@app.post("/api/status")
async def update_bot_status(payload: StatusPayload, x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    
    global current_bot_state
    current_bot_state["status"] = payload.status
    current_bot_state["detail"] = payload.detail or ""
    current_bot_state["tool"] = payload.tool or ""
    
    await manager.broadcast({"type": "status", "data": current_bot_state})
    return {"status": "success", "state": current_bot_state}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)

HTML_CONTENT = """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Jorge's Coder Bot - Live Cloud Stream</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://unpkg.com/lucide@latest"></script>
    <script>
        tailwind.config = {
            darkMode: 'class',
            theme: {
                extend: {
                    colors: {
                        background: "hsl(240 10% 3.9%)",
                        card: "hsl(240 10% 6%)",
                    }
                }
            }
        }
    </script>
    <style>
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: #09090b; }
        ::-webkit-scrollbar-thumb { background: #27272a; border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #3f3f46; }
        @keyframes spin-slow {
            from { transform: rotate(0deg); }
            to { transform: rotate(360deg); }
        }
        .animate-spin-slow {
            animation: spin-slow 2s linear infinite;
        }
    </style>
</head>
<body class="bg-zinc-950 text-zinc-50 min-h-screen font-sans flex flex-col">

    <!-- Header -->
    <header class="border-b border-zinc-800 bg-zinc-900/50 backdrop-blur sticky top-0 z-50 px-6 py-4 flex flex-wrap items-center justify-between gap-4">
        <div class="flex items-center space-x-3">
            <div>
                <h1 class="text-lg font-bold tracking-tight flex items-center gap-2">
                    Jorge's Coder Bot <span class="text-xs bg-blue-950 text-blue-400 border border-blue-900/50 px-2 py-0.5 rounded-full font-mono">NODE1 STREAM</span>
                </h1>
                <p class="text-xs text-zinc-400">Live Log Streamer & Real-time Bot Activity Monitor</p>
            </div>
        </div>

        <div class="flex items-center space-x-4">
            <!-- BOT ACTIVITY INDICATOR (ROLLING ICON WHEN WORKING) -->
            <div id="bot-activity-badge" class="flex items-center space-x-2.5 bg-zinc-900 px-4 py-2 rounded-xl border border-zinc-800 transition-all duration-300">
                <div id="activity-icon-container" class="relative flex items-center justify-center">
                    <i id="activity-icon" data-lucide="check-circle-2" class="w-5 h-5 text-emerald-400"></i>
                </div>
                <div class="flex flex-col">
                    <span id="activity-status-text" class="text-xs font-bold text-emerald-400 tracking-wide uppercase">BOT IDLE</span>
                    <span id="activity-detail-text" class="text-[10px] text-zinc-400 font-mono truncate max-w-[200px]">Ready for commands</span>
                </div>
            </div>

            <!-- WS Connection Status -->
            <div id="connection-status" class="flex items-center space-x-2 bg-zinc-900 px-3 py-1.5 rounded-full border border-zinc-800">
                <span class="w-2.5 h-2.5 bg-amber-500 rounded-full animate-pulse" id="status-dot"></span>
                <span class="text-xs font-medium text-zinc-400" id="status-text">Connecting...</span>
            </div>
        </div>
    </header>

    <!-- Main Content -->
    <main class="flex-1 max-w-7xl w-full mx-auto p-6 space-y-6">
        
        <!-- Info Alert -->
        <div class="bg-blue-950/20 border border-blue-900/30 rounded-xl p-4 flex items-start space-x-3">
            <i data-lucide="info" class="w-5 h-5 text-blue-400 shrink-0 mt-0.5"></i>
            <div class="text-xs text-blue-300 leading-relaxed">
                All log lines are saved permanently to <strong>server_logs.txt</strong> and <strong>C:\BotWorkspace\bot.log</strong>. 
                Log history is preserved forever across page refreshes and server reboots!
            </div>
        </div>

        <!-- Logs Section -->
        <div class="bg-zinc-900/50 border border-zinc-800 rounded-xl flex flex-col h-[650px] overflow-hidden">
            
            <!-- Logs Header / Controls -->
            <div class="border-b border-zinc-800 bg-zinc-900/80 px-5 py-3 flex flex-wrap items-center justify-between gap-4">
                <div class="flex items-center space-x-3">
                    <i data-lucide="terminal" class="w-5 h-5 text-zinc-400"></i>
                    <h2 class="font-semibold text-zinc-200">Preserved Logs</h2>
                    <span class="text-xs bg-zinc-800 text-zinc-400 px-2.5 py-0.5 rounded-full font-mono" id="log-count">0 lines</span>
                </div>
                
                <div class="flex items-center space-x-4">
                    <!-- Search -->
                    <div class="relative">
                        <i data-lucide="search" class="w-4 h-4 text-zinc-500 absolute left-3 top-1/2 -translate-y-1/2"></i>
                        <input type="text" id="log-search" placeholder="Filter logs..." class="bg-zinc-950 border border-zinc-800 rounded-lg pl-9 pr-4 py-1.5 text-sm text-zinc-200 placeholder-zinc-500 focus:outline-none focus:border-zinc-700 w-64">
                    </div>
                    
                    <!-- Level Filter -->
                    <select id="level-filter" class="bg-zinc-950 border border-zinc-800 rounded-lg px-3 py-1.5 text-sm text-zinc-300 focus:outline-none focus:border-zinc-700">
                        <option value="ALL">All Levels</option>
                        <option value="INFO">INFO</option>
                        <option value="WARNING">WARNING</option>
                        <option value="ERROR">ERROR</option>
                    </select>

                    <!-- Auto Scroll Toggle -->
                    <label class="flex items-center space-x-2 cursor-pointer select-none">
                        <input type="checkbox" id="auto-scroll" checked class="sr-only peer">
                        <div class="w-9 h-5 bg-zinc-800 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-zinc-400 after:border-zinc-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-emerald-600 peer-checked:after:bg-white relative"></div>
                        <span class="text-xs font-medium text-zinc-400">Auto-scroll</span>
                    </label>
                </div>
            </div>

            <!-- Logs Terminal -->
            <div id="log-container" class="flex-1 p-5 overflow-y-auto font-mono text-xs space-y-1.5 bg-zinc-950">
                <div class="text-zinc-500 italic text-center py-8">Loading preserved log stream...</div>
            </div>

        </div>

    </main>

    <!-- Footer -->
    <footer class="border-t border-zinc-900 bg-zinc-950 py-6 text-center text-xs text-zinc-500">
        <p>© 2026 Jorge's Coder Bot (NODE1). Log Streamer Deployed on Render.</p>
    </footer>

    <script>
        lucide.createIcons();

        let autoScroll = true;
        let allLogs = [];
        const logContainer = document.getElementById('log-container');
        const autoScrollToggle = document.getElementById('auto-scroll');
        const logSearch = document.getElementById('log-search');
        const levelFilter = document.getElementById('level-filter');
        const statusDot = document.getElementById('status-dot');
        const statusText = document.getElementById('status-text');

        const badge = document.getElementById('bot-activity-badge');
        const iconContainer = document.getElementById('activity-icon-container');
        const statusTextElem = document.getElementById('activity-status-text');
        const detailTextElem = document.getElementById('activity-detail-text');

        autoScrollToggle.addEventListener('change', (e) => {
            autoScroll = e.target.checked;
            if (autoScroll) scrollToBottom();
        });

        // Auto scroll helper
        function scrollToBottom() {
            logContainer.scrollTop = logContainer.scrollHeight;
        }

        function escapeHtml(text) {
            return text
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }

        function updateBotActivityUI(state) {
            if (!state) return;
            const isWorking = state.status === "working";
            
            if (isWorking) {
                badge.className = "flex items-center space-x-2.5 bg-blue-950/40 px-4 py-2 rounded-xl border border-blue-800/60 shadow-lg shadow-blue-950/50 transition-all duration-300";
                iconContainer.innerHTML = '<i data-lucide="loader-2" class="w-5 h-5 text-blue-400 animate-spin"></i>';
                statusTextElem.className = "text-xs font-bold text-blue-400 tracking-wide uppercase flex items-center gap-1.5";
                statusTextElem.innerHTML = '<span>WORKING...</span>';
                detailTextElem.innerText = state.detail || state.tool || "Executing task...";
            } else {
                badge.className = "flex items-center space-x-2.5 bg-zinc-900 px-4 py-2 rounded-xl border border-zinc-800 transition-all duration-300";
                iconContainer.innerHTML = '<i data-lucide="check-circle-2" class="w-5 h-5 text-emerald-400"></i>';
                statusTextElem.className = "text-xs font-bold text-emerald-400 tracking-wide uppercase";
                statusTextElem.innerText = "BOT IDLE";
                detailTextElem.innerText = "Ready for commands";
            }
            lucide.createIcons();
        }

        function renderLogs() {
            const searchTerm = logSearch.value.toLowerCase();
            const selectedLevel = levelFilter.value;
            
            const filteredLogs = allLogs.filter(log => {
                const matchesSearch = log.raw.toLowerCase().includes(searchTerm);
                const matchesLevel = selectedLevel === 'ALL' || log.level === selectedLevel;
                return matchesSearch && matchesLevel;
            });

            document.getElementById('log-count').innerText = `${allLogs.length} lines`;

            const html = filteredLogs.map(log => {
                let colorClass = 'text-zinc-400';
                if (log.level === 'ERROR' || log.level === 'CRITICAL') {
                    colorClass = 'text-red-400 bg-red-950/20 px-1 rounded border-l-2 border-red-500';
                } else if (log.level === 'WARNING') {
                    colorClass = 'text-amber-400 bg-amber-950/10 px-1 rounded border-l-2 border-amber-500';
                } else if (log.level === 'INFO') {
                    colorClass = 'text-zinc-300';
                    if (log.raw.includes('Executing system command') || log.raw.includes('Calling Render API')) {
                        colorClass = 'text-blue-400 font-semibold';
                    } else if (log.raw.includes('Successfully') || log.raw.includes('completed')) {
                        colorClass = 'text-emerald-400';
                    }
                }
                return `<div class="py-0.5 ${colorClass} whitespace-pre-wrap break-all">${escapeHtml(log.raw)}</div>`;
            }).join('');
            
            logContainer.innerHTML = html || '<div class="text-zinc-500 italic text-center py-8">No logs match the current filters.</div>';
            if (autoScroll) scrollToBottom();
        }

        function parseLogLevel(line) {
            let level = "INFO";
            if (line.includes(" - WARNING - ")) level = "WARNING";
            else if (line.includes(" - ERROR - ")) level = "ERROR";
            else if (line.includes(" - CRITICAL - ")) level = "CRITICAL";
            else if (line.includes(" - DEBUG - ")) level = "DEBUG";
            return level;
        }

        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws`;
            const ws = new WebSocket(wsUrl);

            ws.onopen = () => {
                statusDot.className = "w-2.5 h-2.5 bg-emerald-500 rounded-full animate-pulse";
                statusText.innerText = "Live Stream Connected";
            };

            ws.onmessage = (event) => {
                const message = JSON.parse(event.data);
                if (message.type === 'init') {
                    if (message.logs) {
                        allLogs = message.logs.map(line => ({ raw: line, level: parseLogLevel(line) }));
                        renderLogs();
                    }
                    if (message.state) updateBotActivityUI(message.state);
                } else if (message.type === 'logs') {
                    const newLogs = message.data.map(line => ({ raw: line, level: parseLogLevel(line) }));
                    allLogs = allLogs.concat(newLogs);
                    if (allLogs.length > 50000) allLogs = allLogs.slice(-50000);
                    renderLogs();
                } else if (message.type === 'status') {
                    updateBotActivityUI(message.data);
                }
            };

            ws.onclose = () => {
                statusDot.className = "w-2.5 h-2.5 bg-red-500 rounded-full animate-pulse";
                statusText.innerText = "Disconnected. Reconnecting...";
                setTimeout(connectWebSocket, 3000);
            };

            ws.onerror = (err) => {
                console.error('WebSocket error:', err);
                ws.close();
            };
        }

        logSearch.addEventListener('input', renderLogs);
        levelFilter.addEventListener('change', renderLogs);
        connectWebSocket();
    </script>
</body>
</html>
"""
