/**
 * SwarmCoordinator - Durable Object for managing a single compute swarm
 *
 * EDGE CASES & LIFECYCLE NOTES:
 *
 * 1. DO RESTART RECOVERY:
 *    - Durable Objects hibernate after ~10s of inactivity
 *    - On wake: constructor runs again, sessions Map is EMPTY
 *    - Clients auto-reconnect via exponential backoff in WebSocketManager
 *    - Device registry persists in storage, cleanup marks stale devices OFFLINE
 *
 * 2. IN-MEMORY VS STORAGE STATE:
 *    - sessions Map: Ephemeral (rebuilt on reconnection)
 *    - device:* keys: Persistent (survives restarts)
 *    - All writes must use this.state.storage.put()
 *
 * 3. BROADCAST BACKPRESSURE:
 *    - 2s alarm interval broadcasts to ALL connected sockets
 *    - With many devices (>50), consider batching or sampling
 *    - CPU time limits apply (50ms free, 30s paid per invocation)
 *
 * 4. WEBSOCKET LIFECYCLE:
 *    - Half-open sockets detected via heartbeat timeout (60s)
 *    - Graceful close triggers immediate disconnect handler
 *    - Network partitions: Client heartbeat fails, server marks OFFLINE
 *
 * 5. ALARM OPTIMIZATION OPPORTUNITY:
 *    - Currently fires every 2s regardless of activity
 *    - Could cancel alarm when sessions.size === 0 to save CPU/billing
 *    - Trade-off: Slower recovery on first reconnect
 */

import type { DeviceInfo, DeviceCapabilities, Job, SwarmSnapshot } from "./types";

interface WebSocketSession {
  ws: WebSocket;
  persistentId: string;
  deviceName?: string;
}

interface TokenData {
  swarmId: string;
  expiresAt: number;
}

interface DeviceJobQuota {
  deviceId: string;
  weight: number;
  activeJobs: number;
  maxConcurrent: number;
}

export class SwarmCoordinator {
  private state: DurableObjectState;
  private ctx: ExecutionContext;
  
  // In-memory session tracking (lost on migration, rebuilt from connections)
  private sessions = new Map<string, WebSocketSession>();
  
  constructor(state: DurableObjectState, ctx: ExecutionContext) {
    this.state = state;
    this.ctx = ctx;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const persistentId = url.searchParams.get("persistentId")!;
    
    // Accept WebSocket upgrade
    const [client, server] = Object.values(new WebSocketPair());
    
    // Store session
    this.sessions.set(persistentId, {
      ws: server,
      persistentId,
    });

    // Handle messages
    server.accept();
    server.addEventListener("message", async (event) => {
      try {
        const data = JSON.parse(event.data as string);
        await this.handleMessage(persistentId, data, server);
      } catch (err) {
        console.error("Message handling error:", err);
      }
    });

    server.addEventListener("close", async () => {
      this.sessions.delete(persistentId);
      await this.handleDisconnect(persistentId);
    });

    // Send initial connection acknowledgment
    this.broadcastToSession(persistentId, {
      event: "connect",
      data: { status: "connected" }
    });

    // Schedule first broadcast
    this.ctx.waitUntil(this.scheduleBroadcast());
    
    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  private async handleMessage(
    persistentId: string, 
    data: any, 
    ws: WebSocket
  ): Promise<void> {
    const event = data.event;
    const payload = data.data;

    switch (event) {
      case "device:register":
        await this.handleRegister(persistentId, payload);
        break;
      
      case "heartbeat":
        await this.handleHeartbeat(persistentId, payload);
        break;
      
      case "job:request_batch":
        await this.handleJobRequest(persistentId);
        break;
      
      case "job:complete":
        await this.handleJobComplete(persistentId, payload);
        break;
      
      case "cmd:set_run_state":
        await this.handleSetState(payload);
        break;
      
      case "cmd:set_throttle":
        await this.handleSetThrottle(payload);
        break;
      
      case "benchmark:result":
        await this.handleBenchmarkResult(persistentId, payload);
        break;
      
      case "cmd:toggle_device":
        await this.handleToggleDevice(payload);
        break;
      
      case "auth:generate_token":
        await this.handleGenerateToken(persistentId, ws);
        break;
    }
  }

  private async handleRegister(
    persistentId: string, 
    data: { name: string; capabilities: DeviceCapabilities }
  ): Promise<void> {
    const swarmId = this.state.id.toString();
    
    // Get existing device or create new
    const existing = await this.state.storage.get<DeviceInfo>(`device:${persistentId}`);
    
    let type: DeviceInfo["type"] = "DESKTOP";
    if (data.name.toLowerCase().includes("mobile")) type = "MOBILE";
    else if (data.name.toLowerCase().includes("colab")) type = "COLAB";
    else if (data.capabilities.gpuAvailable && data.capabilities.memoryGB > 16) type = "SERVER";

    const device: DeviceInfo = {
      id: persistentId,
      name: data.name,
      type,
      status: "ONLINE",
      capabilities: data.capabilities,
      opsScore: existing?.opsScore || 0,
      totalJobsCompleted: existing?.totalJobsCompleted || 0,
      lastHeartbeat: Date.now(),
      lastUserInteraction: Date.now(),
      swarmId,
    };

    await this.state.storage.put(`device:${persistentId}`, device);
    
    // Register in scheduler
    await this.registerDeviceInScheduler(device);

    this.systemLog("SYS", `Node Registered: ${data.name}`, "AUTH");
    await this.broadcastState();
  }

  private async handleHeartbeat(
    persistentId: string, 
    data?: { lastInteraction: number }
  ): Promise<void> {
    const device = await this.state.storage.get<DeviceInfo>(`device:${persistentId}`);
    if (!device) return;

    device.lastHeartbeat = Date.now();
    if (data?.lastInteraction) {
      device.lastUserInteraction = data.lastInteraction;
    }
    
    if (device.status === "OFFLINE" || device.status === "REGISTERED") {
      device.status = "ONLINE";
    }

    await this.state.storage.put(`device:${persistentId}`, device);
  }

  private async handleJobRequest(persistentId: string): Promise<void> {
    const runState = await this.state.storage.get<SwarmSnapshot["runState"]>("swarm:state");
    if (runState !== "RUNNING") return;

    const device = await this.state.storage.get<DeviceInfo>(`device:${persistentId}`);
    if (!device || device.status === "OFFLINE") return;

    const batch = await this.getJobBatchForDevice(device);
    
    if (batch.length > 0) {
      this.broadcastToSession(persistentId, {
        event: "job:batch",
        data: batch
      });
    }
  }

  private async handleJobComplete(
    persistentId: string, 
    payload: { chunkId: string; error?: string; durationMs?: number }
  ): Promise<void> {
    // Release slot in scheduler
    await this.releaseJobSlot(persistentId, payload.durationMs);

    if (!payload.error) {
      // Increment completed count
      const current = await this.state.storage.get<number>("swarm:completed") || 0;
      await this.state.storage.put("swarm:completed", current + 1);

      // Update device stats
      const device = await this.state.storage.get<DeviceInfo>(`device:${persistentId}`);
      if (device) {
        device.totalJobsCompleted++;
        await this.state.storage.put(`device:${persistentId}`, device);
      }
    } else {
      this.systemLog("ERR", `Job ${payload.chunkId} failed on ${persistentId}`, "COMPUTE");
    }

    // Broadcast updated state
    await this.broadcastState();
  }

  private async handleSetState(state: SwarmSnapshot["runState"]): Promise<void> {
    await this.state.storage.put("swarm:state", state);
    this.systemLog("SYS", `Swarm state changed to ${state}`, "MASTER");
    await this.broadcastState();
  }

  private async handleSetThrottle(value: number): Promise<void> {
    await this.state.storage.put("swarm:throttle", value);
    
    // Broadcast to all connected clients
    this.broadcastToAll({
      event: "swarm:throttle_sync",
      data: value
    });
    
    await this.broadcastState();
  }

  private async handleBenchmarkResult(
    persistentId: string, 
    data: { score: number }
  ): Promise<void> {
    const device = await this.state.storage.get<DeviceInfo>(`device:${persistentId}`);
    if (device) {
      device.opsScore = data.score;
      await this.state.storage.put(`device:${persistentId}`, device);
      await this.registerDeviceInScheduler(device);
    }

    this.systemLog("CPU", `Benchmark: ${data.score.toLocaleString()} OPS`, persistentId);
    await this.broadcastState();
  }

  private async handleToggleDevice(payload: { id: string; enabled: boolean }): Promise<void> {
    const device = await this.state.storage.get<DeviceInfo>(`device:${payload.id}`);
    if (device) {
      device.status = payload.enabled ? "ONLINE" : "DISABLED";
      await this.state.storage.put(`device:${payload.id}`, device);
      
      this.systemLog(
        payload.enabled ? "SYS" : "WARN",
        `Node ${payload.id.slice(0, 8)} was ${payload.enabled ? "enabled" : "disabled"} by Master`,
        "ORCHESTRATOR"
      );
      
      await this.broadcastState();
    }
  }

  private async handleGenerateToken(persistentId: string, ws: WebSocket): Promise<void> {
    const swarmId = this.state.id.toString();
    
    // Check rate limit
    const now = Date.now();
    const key = `ratelimit:${swarmId}`;
    const generations = await this.state.storage.get<number[]>(key) || [];
    const oneMinuteAgo = now - 60000;
    const recent = generations.filter(t => t > oneMinuteAgo);

    if (recent.length >= 5) {
      this.systemLog("WARN", "Invite code generation rate limited (5/min)", "AUTH");
      ws.send(JSON.stringify({ event: "auth:token", data: "" }));
      return;
    }

    recent.push(now);
    await this.state.storage.put(key, recent);

    // Generate token
    const token = Math.random().toString(36).substring(2, 8).toUpperCase();
    const tokenData: TokenData = {
      swarmId,
      expiresAt: now + (24 * 60 * 60 * 1000), // 24 hours
    };

    await this.state.storage.put(`token:${token}`, tokenData);
    
    this.systemLog("SYS", `Generated new invite code: ${token}`, "AUTH");
    ws.send(JSON.stringify({ event: "auth:token", data: token }));
  }

  private async handleDisconnect(persistentId: string): Promise<void> {
    this.systemLog("NET", `Node offline: ${persistentId.slice(0, 8)}`, "GATEWAY");
    
    // Mark device as offline (but keep in storage for reconnection)
    const device = await this.state.storage.get<DeviceInfo>(`device:${persistentId}`);
    if (device && device.status !== "DISABLED") {
      device.status = "OFFLINE";
      await this.state.storage.put(`device:${persistentId}`, device);
      await this.broadcastState();
    }
  }

  // Job Scheduler Logic
  private async registerDeviceInScheduler(device: DeviceInfo): Promise<void> {
    const weight = this.calculateWeight(device);
    
    const quota: DeviceJobQuota = {
      deviceId: device.id,
      weight,
      activeJobs: 0,
      maxConcurrent: Math.max(10, (device.capabilities.cpuCores || 4) * 4),
    };

    await this.state.storage.put(`quota:${device.id}`, quota);
    
    // Update total weight
    const weights = await this.state.storage.get<Map<string, number>>("scheduler:weights") || new Map();
    weights.set(device.id, weight);
    await this.state.storage.put("scheduler:weights", weights);
  }

  private calculateWeight(device: DeviceInfo): number {
    const cpu = (device.capabilities.cpuCores || 4) * 10;
    const gpu = device.capabilities.gpuAvailable ? 50 : 0;
    const score = Math.min((device.opsScore || 0) / 1000, 100);
    return Math.max(1, cpu + gpu + score);
  }

  private async getJobBatchForDevice(device: DeviceInfo): Promise<Job[]> {
    const quota = await this.state.storage.get<DeviceJobQuota>(`quota:${device.id}`);
    if (!quota) {
      await this.registerDeviceInScheduler(device);
      return this.getJobBatchForDevice(device);
    }

    const availableSlots = quota.maxConcurrent - quota.activeJobs;
    if (availableSlots <= 0) return [];

    // Get job queue
    const queue = await this.state.storage.get<Job[]>("job:queue") || [];
    if (queue.length === 0) {
      // Generate jobs if empty
      await this.generateJobs();
      return this.getJobBatchForDevice(device);
    }

    const weights = await this.state.storage.get<Map<string, number>>("scheduler:weights") || new Map();
    const totalWeight = Array.from(weights.values()).reduce((a, b) => a + b, 0);
    const weight = weights.get(device.id) || 1;
    
    const share = Math.max(1, Math.floor((weight / Math.max(1, totalWeight)) * 30));
    const batchSize = Math.min(share, availableSlots, queue.length);

    const batch: Job[] = [];
    const preferred = device.capabilities.gpuAvailable ? "MAT_MUL" : "MATH_STRESS";

    for (let i = 0; i < batchSize; i++) {
      let idx = queue.findIndex(j => j.type === preferred);
      if (idx === -1) idx = 0;
      if (idx < queue.length) {
        batch.push(queue.splice(idx, 1)[0]);
        quota.activeJobs++;
      }
    }

    await this.state.storage.put(`quota:${device.id}`, quota);
    await this.state.storage.put("job:queue", queue);

    return batch;
  }

  private async releaseJobSlot(deviceId: string, durationMs?: number): Promise<void> {
    const quota = await this.state.storage.get<DeviceJobQuota>(`quota:${deviceId}`);
    if (quota && quota.activeJobs > 0) {
      quota.activeJobs--;
      
      if (durationMs && durationMs > 0) {
        const latencyImpact = durationMs > 2000 ? 0.95 : 1.02;
        quota.weight *= latencyImpact;
      }
      
      await this.state.storage.put(`quota:${deviceId}`, quota);
    }
  }

  private async generateJobs(): Promise<void> {
    const queue = await this.state.storage.get<Job[]>("job:queue") || [];
    if (queue.length > 5000) return;

    for (let i = 0; i < 100; i++) {
      const isGpuTask = Math.random() > 0.7;
      queue.push({
        id: `job-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`,
        type: isGpuTask ? "MAT_MUL" : "MATH_STRESS",
        complexity: Math.floor(Math.random() * 10) + 1,
        data: isGpuTask ? { size: 1024 } : { iterations: 100000 },
      });
    }

    await this.state.storage.put("job:queue", queue);
  }

  // State Broadcasting
  private async broadcastState(): Promise<void> {
    const swarmId = this.state.id.toString();
    
    // Get all devices
    const devices: Record<string, DeviceInfo> = {};
    const list = await this.state.storage.list<DeviceInfo>({ prefix: "device:" });
    let totalOpsScore = 0;
    let totalCores = 0;
    let totalMemory = 0;
    let totalGPUs = 0;
    let onlineCount = 0;

    for (const [key, device] of list) {
      devices[device.id] = device;
      
      if (device.status !== "OFFLINE" && device.status !== "DISABLED") {
        totalOpsScore += device.opsScore || 0;
        totalCores += device.capabilities.cpuCores;
        totalMemory += device.capabilities.memoryGB;
        if (device.capabilities.gpuAvailable) totalGPUs++;
        onlineCount++;
      }
    }

    const runState = await this.state.storage.get<SwarmSnapshot["runState"]>("swarm:state") || "STOPPED";
    const completedCount = await this.state.storage.get<number>("swarm:completed") || 0;
    const currentThrottle = await this.state.storage.get<number>("swarm:throttle") || 40;
    const queue = await this.state.storage.get<Job[]>("job:queue") || [];

    const snapshot: SwarmSnapshot = {
      runState,
      devices,
      stats: {
        totalJobs: completedCount + queue.length,
        activeJobs: runState === "RUNNING" ? onlineCount : 0,
        pendingJobs: queue.length,
        completedJobs: completedCount,
        globalVelocity: runState === "RUNNING" ? Math.round(totalOpsScore * (currentThrottle / 100)) : 0,
        globalThrottle: currentThrottle,
      },
      resources: {
        totalCores,
        totalMemory,
        totalGPUs,
        onlineCount,
      },
    };

    this.broadcastToAll({
      event: "swarm:snapshot",
      data: snapshot
    });
  }

  private async scheduleBroadcast(): Promise<void> {
    // Schedule periodic broadcasts via alarm
    const alarm = await this.state.storage.getAlarm();
    if (alarm === null) {
      await this.state.storage.setAlarm(Date.now() + 2000);
    }
  }

  async alarm(): Promise<void> {
    /**
     * Called every 2 seconds by Durable Object alarm system
     * 
     * PERFORMANCE NOTE: This fires regardless of activity. With 0 connected devices,
     * we're still burning CPU every 2s. Consider optimization:
     * 
     * if (this.sessions.size === 0) {
     *   await this.state.storage.deleteAlarm();  // Hibernate
     *   return;
     * }
     * 
     * Trade-off: First reconnection will have stale data until next registration
     */
    
    // Periodic tasks
    await this.broadcastState();
    await this.cleanup();
    
    // Reschedule
    await this.state.storage.setAlarm(Date.now() + 2000);
  }

  private async cleanup(): Promise<void> {
    const now = Date.now();
    const list = await this.state.storage.list<DeviceInfo>({ prefix: "device:" });

    for (const [key, device] of list) {
      const diff = now - device.lastHeartbeat;
      
      if (diff > 300000) { // 5 minutes - delete
        await this.state.storage.delete(key);
      } else if (device.status !== "DISABLED" && diff > 60000) { // 1 minute - mark offline
        device.status = "OFFLINE";
        await this.state.storage.put(key, device);
      }
    }
  }

  // Helpers
  private broadcastToSession(persistentId: string, message: any): void {
    const session = this.sessions.get(persistentId);
    if (session?.ws.readyState === WebSocket.READY_STATE_OPEN) {
      session.ws.send(JSON.stringify(message));
    }
  }

  private broadcastToAll(message: any): void {
    const data = JSON.stringify(message);
    for (const session of this.sessions.values()) {
      if (session.ws.readyState === WebSocket.READY_STATE_OPEN) {
        session.ws.send(data);
      }
    }
  }

  private systemLog(level: string, message: string, source: string = "CORE"): void {
    console.log(`[${level}] [${source}] ${message}`);
    
    this.broadcastToAll({
      event: "sys:log",
      data: {
        level,
        message,
        timestamp: Date.now(),
        source
      }
    });
  }
}
