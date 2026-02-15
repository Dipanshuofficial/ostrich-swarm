import {
  type DeviceInfo,
  type DeviceCapabilities,
  type DeviceType,
} from "../core/types";

export class DeviceManager {
  private devices = new Map<string, DeviceInfo>();
  // 60s for offline (allows for long reloads/bad signal), 5mins for full deletion
  private readonly OFFLINE_THRESHOLD = 60000;
  private readonly DELETE_THRESHOLD = 300000;
  constructor() {
    setInterval(() => this.cleanup(), 5000);
  }

  public register(
    id: string,
    name: string,
    caps: DeviceCapabilities,
    swarmId: string,
  ) {
    const existing = this.devices.get(id);
    let type: DeviceType = "DESKTOP";

    if (name.toLowerCase().includes("mobile")) {
      type = "MOBILE";
    } else if (name.toLowerCase().includes("colab")) {
      type = "COLAB";
    } else if (caps.gpuAvailable && caps.memoryGB > 16) {
      type = "SERVER";
    }
    this.devices.set(id, {
      id,
      name,
      type,
      status: "ONLINE", // Move to ONLINE immediately if registering/re-registering
      capabilities: caps,
      opsScore: existing?.opsScore || 0,
      totalJobsCompleted: existing?.totalJobsCompleted || 0,
      lastHeartbeat: Date.now(),
      lastUserInteraction: Date.now(),
      swarmId,
    });
  }

  public heartbeat(id: string, data?: { lastInteraction: number }) {
    const device = this.devices.get(id);
    if (!device) return;

    device.lastHeartbeat = Date.now();
    if (data?.lastInteraction)
      device.lastUserInteraction = data.lastInteraction;

    // If they heartbeat, they are ONLINE
    if (device.status === "OFFLINE" || device.status === "REGISTERED") {
      device.status = "ONLINE";
    }
  }

  public toggleDevice(id: string, enabled: boolean) {
    const device = this.devices.get(id);
    if (device) device.status = enabled ? "ONLINE" : "DISABLED";
  }

  public updateScore(id: string, score: number) {
    const device = this.devices.get(id);
    if (device) device.opsScore = score;
  }

  public getDevicesBySwarm(swarmId: string) {
    return Array.from(this.devices.values()).filter(
      (d) => d.swarmId === swarmId,
    );
  }

  public getAvailableResources(swarmId: string) {
    let [totalCores, totalMemory, totalGPUs, onlineCount] = [0, 0, 0, 0];

    this.getDevicesBySwarm(swarmId).forEach((d) => {
      if (
        d.status === "ONLINE" ||
        d.status === "BUSY" ||
        d.status === "REGISTERED"
      ) {
        totalCores += d.capabilities.cpuCores;
        totalMemory += d.capabilities.memoryGB;
        if (d.capabilities.gpuAvailable) totalGPUs++;
        onlineCount++;
      }
    });

    return { totalCores, totalMemory, totalGPUs, onlineCount };
  }

  public getDevice(id: string) {
    return this.devices.get(id);
  }

  public remove(id: string) {
    this.devices.delete(id);
  }

  private cleanup() {
    const now = Date.now();
    this.devices.forEach((device, id) => {
      const diff = now - device.lastHeartbeat;

      if (diff > this.DELETE_THRESHOLD) {
        this.devices.delete(id);
      } else if (
        device.status !== "DISABLED" &&
        diff > this.OFFLINE_THRESHOLD
      ) {
        device.status = "OFFLINE";
      }
    });
  }
}
