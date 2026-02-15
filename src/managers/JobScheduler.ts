import { type Job, type DeviceInfo } from "../core/types";

interface DeviceJobQuota {
  deviceId: string;
  weight: number;
  activeJobs: number;
  maxConcurrent: number;
}

export class JobScheduler {
  private jobQueue: Job[] = [];
  private deviceQuotas = new Map<string, DeviceJobQuota>();
  private deviceWeights = new Map<string, number>();
  private totalWeight = 0;
  private readonly MAX_QUEUE_SIZE = 5000;

  constructor() {
    setInterval(() => this.generateJobs(), 1000);
  }

  private generateJobs() {
    if (this.jobQueue.length > this.MAX_QUEUE_SIZE) return;
    for (let i = 0; i < 100; i++) {
      const isGpuTask = Math.random() > 0.7;
      this.jobQueue.push({
        id: `job-${Date.now()}-${Math.random().toString(36).substring(2, 7)}`,
        type: isGpuTask ? "MAT_MUL" : "MATH_STRESS",
        complexity: Math.floor(Math.random() * 10) + 1,
        data: isGpuTask ? { size: 1024 } : { iterations: 100000 },
      });
    }
  }

  private calculateWeight(device: DeviceInfo): number {
    const cpu = (device.capabilities.cpuCores || 4) * 10;
    const gpu = device.capabilities.gpuAvailable ? 50 : 0;
    const score = Math.min((device.opsScore || 0) / 1000, 100);
    return Math.max(1, cpu + gpu + score);
  }

  public registerDevice(device: DeviceInfo) {
    const weight = this.calculateWeight(device);
    const old = this.deviceWeights.get(device.id) || 0;
    this.totalWeight += weight - old;
    this.deviceWeights.set(device.id, weight);

    this.deviceQuotas.set(device.id, {
      deviceId: device.id,
      weight,
      activeJobs: 0,
      maxConcurrent: Math.max(10, (device.capabilities.cpuCores || 4) * 4),
    });
  }

  public getJobBatchForDevice(
    device: DeviceInfo,
    requestedCount: number = 10,
  ): Job[] {
    const quota = this.deviceQuotas.get(device.id);
    if (!quota) {
      this.registerDevice(device);
      return this.getJobBatchForDevice(device, requestedCount);
    }

    const availableSlots = quota.maxConcurrent - quota.activeJobs;
    if (availableSlots <= 0) return [];

    const weight = this.deviceWeights.get(device.id) || 1;
    const share = Math.max(
      1,
      Math.floor((weight / Math.max(1, this.totalWeight)) * requestedCount * 3),
    );

    const batchSize = Math.min(share, availableSlots, this.jobQueue.length);

    const batch: Job[] = [];
    const preferred = device.capabilities.gpuAvailable
      ? "MAT_MUL"
      : "MATH_STRESS";

    for (let i = 0; i < batchSize; i++) {
      let idx = this.jobQueue.findIndex((j) => j.type === preferred);
      if (idx === -1) idx = 0;
      if (idx < this.jobQueue.length) {
        batch.push(this.jobQueue.splice(idx, 1)[0]);
        quota.activeJobs++;
      }
    }
    return batch;
  }

  public releaseJobSlot(deviceId: string, durationMs?: number) {
    const quota = this.deviceQuotas.get(deviceId);
    if (quota && quota.activeJobs > 0) {
      quota.activeJobs--;

      // If we got duration data, we could refine the weight here
      if (durationMs && durationMs > 0) {
        // Example: Penalize weight if jobs are taking > 2000ms
        const latencyImpact = durationMs > 2000 ? 0.95 : 1.02;
        quota.weight *= latencyImpact;
      }
    }
  }

  public getQueueStats() {
    return {
      pending: this.jobQueue.length,
      active: Array.from(this.deviceQuotas.values()).reduce(
        (sum, q) => sum + q.activeJobs,
        0,
      ),
    };
  }
}
