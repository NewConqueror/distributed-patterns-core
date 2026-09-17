import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';

const client = new grpc.Client();
client.load(['../../proto', 'proto'], 'telemetry/v1/telemetry.proto');

export const options = {
  scenarios: {
    telemetry_stream: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '3s', target: 500 },
        { duration: '10s', target: 1000 },
        { duration: '3s', target: 0 },
      ],
      gracefulRampDown: '2s',
    },
  },
  thresholds: {
    grpc_req_duration: ['p(95)<25', 'p(99)<50'], // 95% of requests under 25ms, 99% under 50ms
  },
};

export default function () {
  client.connect('localhost:50051', {
    plaintext: true,
  });

  const driverId = `k6-driver-${__VU}`;
  const now = Date.now();

  // Perform radius query test
  const queryResponse = client.invoke('telemetry.v1.TelemetryService/GetNearbyDrivers', {
    latitude: 41.0082,
    longitude: 28.9784,
    radius_km: 5.0,
    limit: 10,
  });

  check(queryResponse, {
    'status is OK': (r) => r && r.status === grpc.StatusOK,
  });

  client.close();
  sleep(0.1);
}
