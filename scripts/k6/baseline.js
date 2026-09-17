import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  scenarios: {
    baseline: {
      executor: 'ramping-vus',
      startVUs: 10,
      stages: [
        { duration: '5s', target: 50 },
        { duration: '10s', target: 100 },
        { duration: '5s', target: 0 },
      ],
      gracefulRampDown: '2s',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<100', 'p(99)<200'], // 99% requests under 200ms
    http_req_failed: ['rate<0.01'],                 // Less than 1% errors
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://127.0.0.1:8080';

export default function () {
  const res = http.get(`${BASE_URL}/api/v1/items`);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'backend server header present': (r) => r.headers['X-Backend-Server'] !== undefined,
  });

  sleep(0.01);
}
