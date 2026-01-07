// Using mqtt.js via npm: specifier
import mqtt from "npm:mqtt@5";

let client: mqtt.MqttClient | null = null;

export function getClient(): mqtt.MqttClient {
  if (!client) {
    throw new Error("MQTT not connected. Call cp.start() first.");
  }
  return client;
}

export async function connect(brokerUrl: string, clientId: string): Promise<mqtt.MqttClient> {
  return new Promise((resolve, reject) => {
    client = mqtt.connect(`mqtt://${brokerUrl}`, {
      clientId,
      clean: true,
    });

    client.on("connect", () => {
      console.log(`[mqtt] Connected as ${clientId}`);
      resolve(client!);
    });

    client.on("error", (err) => {
      console.error(`[mqtt] Error:`, err);
      reject(err);
    });
  });
}

export function publish(topic: string, message: string | object): void {
  const client = getClient();
  const payload = typeof message === "string" ? message : JSON.stringify(message);
  client.publish(topic, payload);
}

/**
 * Check if a topic matches an MQTT wildcard pattern.
 * - `+` matches exactly one level (single-level wildcard)
 * - `#` matches zero or more levels (multi-level wildcard, must be at end)
 */
function topicMatches(pattern: string, topic: string): boolean {
  // Exact match
  if (pattern === topic) return true;

  const patternParts = pattern.split('/');
  const topicParts = topic.split('/');

  for (let i = 0; i < patternParts.length; i++) {
    const p = patternParts[i];

    // Multi-level wildcard - matches everything from here
    if (p === '#') {
      return true; // # must be last, matches rest
    }

    // No more topic parts but pattern continues (and it's not #)
    if (i >= topicParts.length) {
      return false;
    }

    // Single-level wildcard - matches any single level
    if (p === '+') {
      continue; // matches this level, continue
    }

    // Literal match required
    if (p !== topicParts[i]) {
      return false;
    }
  }

  // Pattern exhausted - topic must also be exhausted
  return patternParts.length === topicParts.length;
}

export function subscribe(topic: string, handler: (topic: string, payload: Buffer) => void): void {
  const client = getClient();
  client.subscribe(topic);
  client.on("message", (t, p) => {
    if (topicMatches(topic, t)) {
      handler(t, p);
    }
  });
}
