export class AuthManager {
  private activeTokens = new Map<
    string,
    { swarmId: string; expiresAt: number }
  >();
  private readonly TOKEN_TTL_MS = 24 * 60 * 60 * 1000;
  private tokenGenerationLog = new Map<string, number[]>(); // swarmId -> timestamps
  constructor() {
    setInterval(() => this.cleanup(), 30000);
  }

  /**
   * Generates a unique 6-digit alphanumeric token for a specific swarm.
   */
  public generateToken(swarmId: string): string | null {
    const now = Date.now();
    let generations = this.tokenGenerationLog.get(swarmId) || [];
    const oneMinuteAgo = now - 60000;
    generations = generations.filter((t) => t > oneMinuteAgo);

    if (generations.length >= 5) {
      return null; // Rate limited
    }

    generations.push(now);
    this.tokenGenerationLog.set(swarmId, generations);

    const token = Math.random().toString(36).substring(2, 8).toUpperCase();
    this.activeTokens.set(token, {
      swarmId,
      expiresAt: Date.now() + this.TOKEN_TTL_MS,
    });

    return token;
  }

  /**
   * Validates a token and returns the associated swarmId.
   * This is the "Party Join" mechanism.
   */
  public validateToken(token: string): string | null {
    const data = this.activeTokens.get(token);

    if (!data) return null;

    if (Date.now() > data.expiresAt) {
      this.activeTokens.delete(token);
      return null;
    }

    return data.swarmId;
  }

  private cleanup() {
    const now = Date.now();
    for (const [token, data] of this.activeTokens.entries()) {
      if (now > data.expiresAt) this.activeTokens.delete(token);
    }
  }
}
