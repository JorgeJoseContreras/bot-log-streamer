import os
from typing import List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Header, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI(title="Jorge's Bot Log Streamer")

# Simple API key for security
API_KEY = os.getenv("LOG_STREAMER_API_KEY", "super-secret-key")

# Store active websocket connections
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                # Handle broken connections gracefully
                pass

manager = ConnectionManager()

class LogPayload(BaseModel):
    logs: List[str]

@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    # We can embed the HTML directly or use Jinja2. Direct HTML is simpler and self-contained.
    return HTML_CONTENT

@app.post("/api/logs")
async def receive_logs(payload: LogPayload, x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    
    # Broadcast logs to all connected websockets
    await manager.broadcast({"type": "logs", "data": payload.logs})
    return {"status": "success", "broadcasted": len(payload.logs)}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive and listen for any client messages (though we don't expect any)
            data = await websocket.receive_text()
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
                        border: "hsl(240 5.9% 90%)",
                        background: "hsl(240 10% 3.9%)",
                        foreground: "hsl(0 0% 98%)",
                        card: "hsl(240 10% 6%)",
                        "card-foreground": "hsl(0 0% 98%)",
                    }
                }
            }
        }
    </script>
    <style>
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        ::-webkit-scrollbar-track {
            background: #09090b;
        }
        ::-webkit-scrollbar-thumb {
            background: #27272a;
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #3f3f46;
        }
        @keyframes pulse-slow {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.4; }
        }
        .animate-pulse-slow {
            animation: pulse-slow 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
        }
    </style>
</head>
<body class="bg-zinc-950 text-zinc-50 min-h-screen font-sans flex flex-col">

    <!-- Header -->
    <header class="border-b border-zinc-800 bg-zinc-900/50 backdrop-blur sticky top-0 z-50 px-6 py-4 flex items-center justify-between">
        <div class="flex items-center space-x-3">
            <div class="bg-blue-500/10 p-2 rounded-lg border border-blue-500/20 text-blue-400">
                <i data-lucide="cloud-lightning" class="w-6 h-6"></i>
            </div>
            <div>
                <h1 class="text-lg font-bold tracking-tight flex items-center gap-2">
                    Jorge's Coder Bot <span class="text-xs bg-blue-950 text-blue-400 border border-blue-900/50 px-2 py-0.5 rounded-full font-mono">CLOUD STREAM</span>
                </h1>
                <p class="text-xs text-zinc-400">Live Log Streamer deployed on Render</p>
            </div>
        </div>
        <div class="flex items-center space-x-4">
            <div id="connection-status" class="flex items-center space-x-2 bg-zinc-900 px-3 py-1.5 rounded-full border border-zinc-800">
                <span class="w-2.5 h-2.5 bg-amber-500 rounded-full animate-pulse-slow" id="status-dot"></span>
                <span class="text-xs font-medium text-zinc-300" id="status-text">Connecting...</span>
            </div>
        </div>
    </header>

    <!-- Main Content -->
    <main class="flex-1 max-w-7xl w-full mx-auto p-6 space-y-6">
        
        <!-- Info Alert -->
        <div class="bg-blue-950/20 border border-blue-900/30 rounded-xl p-4 flex items-start space-x-3">
            <i data-lucide="info" class="w-5 h-5 text-blue-400 shrink-0 mt-0.5"></i>
            <div class="text-xs text-blue-300 leading-relaxed">
                This page streams live logs directly from Jorge's Coder Bot running locally on the <strong>NODE1 Mini PC</strong>. 
                The local bot pushes log lines in real-time to this Render instance, which broadcasts them to your browser via WebSockets.
            </div>
        </div>

        <!-- Logs Section -->
        <div class="bg-zinc-900/50 border border-zinc-800 rounded-xl flex flex-col h-[650px] overflow-hidden">
            
            <!-- Logs Header / Controls -->
            <div class="border-b border-zinc-800 bg-zinc-900/80 px-5 py-3 flex flex-wrap items-center justify-between gap-4">
                <div class="flex items-center space-x-3">
                    <i data-lucide="terminal" class="w-5 h-5 text-zinc-400"></i>
                    <h2 class="font-semibold text-zinc-200">Live Log Stream</h2>
                    <span class="text-xs bg-zinc-800 text-zinc-400 px-2 py-0.5 rounded-full font-mono" id="log-count">0 lines</span>
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
                <div class="text-zinc-500 italic text-center py-8">Waiting for logs from local bot...</div>
            </div>

        </div>

    </main>

    <!-- Footer -->
    <footer class="border-t border-zinc-900 bg-zinc-950 py-6 text-center text-xs text-zinc-500">
        <p>© 2026 Jorge's Coder Bot. Deployed on Render. Connected to NODE1.</p>
    </footer>

    <script>
        // Initialize Lucide Icons
        lucide.createIcons();

        let autoScroll = true;
        let allLogs = [];
        const logContainer = document.getElementById('log-container');
        const autoScrollToggle = document.getElementById('auto-scroll');
        const logSearch = document.getElementById('log-search');
        const levelFilter = document.getElementById('level-filter');
        const statusDot = document.getElementById('status-dot');
        const statusText = document.getElementById('status-text');

        autoScrollToggle.addEventListener('change', (e) => {
            autoScroll = e.target.checked;
            if (autoScroll) {
                scrollToBottom();
            }
        });

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
            
            if (autoScroll) {
                scrollToBottom();
            }
        }

        function parseLogLevel(line) {
            let level = "INFO";
            if (line.includes(" - WARNING - ")) {
                level = "WARNING";
            } else if (line.includes(" - ERROR - ")) {
                level = "ERROR";
            } else if (line.includes(" - CRITICAL - ")) {
                level = "CRITICAL";
            } else if (line.includes(" - DEBUG - ")) {
                level = "DEBUG";
            }
            return level;
        }

        // Connect to WebSocket
        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws`;
            const ws = new WebSocket(wsUrl);

            ws.onopen = () => {
                statusDot.className = "w-2.5 h-2.5 bg-emerald-500 rounded-full animate-pulse-slow";
                statusText.innerText = "Live Stream Connected";
            };

            ws.onmessage = (event) => {
                const message = JSON.parse(event.data);
                if (message.type === 'logs') {
                    const newLogs = message.data.map(line => ({
                        raw: line,
                        level: parseLogLevel(line)
                    }));
                    allLogs = allLogs.concat(newLogs);
                    
                    // Keep only last 2000 lines to prevent browser memory issues
                    if (allLogs.length > 2000) {
                        allLogs = allLogs.slice(-2000);
                    }
                    
                    renderLogs();
                }
            };

            ws.onclose = () => {
                statusDot.className = "w-2.5 h-2.5 bg-red-500 rounded-full animate-pulse-slow";
                statusText.innerText = "Disconnected. Reconnecting...";
                setTimeout(connectWebSocket, 3000);
            };

            ws.onerror = (err) => {
                console.error('WebSocket error:', err);
                ws.close();
            };
        }

        // Event listeners
        logSearch.addEventListener('input', renderLogs);
        levelFilter.addEventListener('change', renderLogs);

        // Start connection
        connectWebSocket();
    </script>
</body>
</html>
"""
