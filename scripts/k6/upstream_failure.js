import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: 10,
  duration: '6s',
  thresholds: {
    http_req_failed: ['rate<0.15'], // Transient failures during backend crash within 15% threshold
  },
};

const GATEWAY_URL = __ENV.GATEWAY_URL || 'http://127.0.0.1:8080';
const BACKEND1_URL = __ENV.BACKEND1_URL || 'http://127.0.0.1:8081';

export default function (data) {
  // At iteration 10 on VU 1, toggle backend-1 to unhealthy
  if (__VU === 1 && __ITER === 10) {
    console.log('>>> Simulating backend-1 failure by toggling health to UNHEALTHY <<<');
    http.get(`${BACKEND1_URL}/health/toggle`);
  }

  const res = http.get(`${GATEWAY_URL}/api/v1/items`);

  check(res, {
    'request succeeded': (r) => r.status === 200,
    'backend handled request': (r) => r.headers['X-Backend-Server'] !== undefined,
  });

  sleep(0.05);
}

export function teardown() {
  // Restore backend-1 health state after test
  console.log('>>> Teardown: Restoring backend-1 health <<<');
  http.get(`${BACKEND1_URL}/health/toggle`);
}
