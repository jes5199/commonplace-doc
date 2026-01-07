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

export function subscribe(topic: string, handler: (topic: string, payload: Buffer) => void): void {
  const client = getClient();
  client.subscribe(topic);
  client.on("message", (t, p) => {
    if (t === topic || topic.endsWith("#") && t.startsWith(topic.slice(0, -1))) {
      handler(t, p);
    }
  });
}
