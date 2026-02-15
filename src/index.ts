import { SwarmCoordinator } from "./SwarmCoordinator";

export { SwarmCoordinator };

export interface Env {
  SWARM_COORDINATOR: DurableObjectNamespace<SwarmCoordinator>;
  OSTRICH_KV: KVNamespace;
}

function getCorsHeaders(): Headers {
  const headers = new Headers();
  headers.set("Access-Control-Allow-Origin", "*");
  headers.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  headers.set("Access-Control-Allow-Headers", "Content-Type");
  return headers;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const corsHeaders = getCorsHeaders();
    
    // Handle CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    // Health check
    if (path === "/health") {
      return new Response(JSON.stringify({ status: "ok" }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    // WebSocket upgrade endpoint
    if (path === "/ws") {
      const persistentId = url.searchParams.get("persistentId");
      const token = url.searchParams.get("token");

      if (!persistentId) {
        return new Response("Missing persistentId", { 
          status: 400,
          headers: corsHeaders 
        });
      }

      // Determine swarm ID based on token
      let swarmId: string;
      
      if (!token || token === "null" || token === "undefined" || token === "") {
        // Master mode: Host joins their own swarm
        swarmId = persistentId;
      } else {
        // Worker mode: Need to validate token
        // For now, use token as swarm ID (simplified - in production validate against KV)
        swarmId = token;
      }

      // Get or create the Durable Object for this swarm
      const id = env.SWARM_COORDINATOR.idFromName(swarmId);
      const swarm = env.SWARM_COORDINATOR.get(id);

      // Upgrade to WebSocket
      return swarm.fetch(request);
    }

    return new Response("Not Found", { 
      status: 404,
      headers: corsHeaders 
    });
  },
};
