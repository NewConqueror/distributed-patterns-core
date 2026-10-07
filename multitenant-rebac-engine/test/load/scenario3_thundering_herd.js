import http from 'k6/http';
import { check } from 'k6';

// Scenario 3: Thundering Herd Test
// Objective: 500 concurrent requests hit the exact same uncached permission simultaneously.
// Singleflight in Go ensures only 1 request queries PostgreSQL, while 499 share the response.

export const options = {
  scenarios: {
    thundering_herd_spike: {
      executor: 'per-vu-iterations',
      vus: 500,        // 500 Virtual Users simultaneously
      iterations: 1,   // Each VU sends exactly 1 request at the same instant
      maxDuration: '10s',
    },
  },
  thresholds: {
    http_req_failed: ['rate==0'],       // 0% failures allowed under thundering herd
    http_req_duration: ['p(99)<250'],   // All collapsed requests must complete swiftly
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:8080';
const DEFAULT_TENANT_ID = '00000000-0000-0000-0000-000000000001';

// Same target permission for all 500 concurrent VUs
const payload = JSON.stringify({
  tenant_id: DEFAULT_TENANT_ID,
  subject: 'user:herd-test-subject',
  permission: 'viewer',
  object: 'document:herd-test-document',
});

const params = {
  headers: {
    'Content-Type': 'application/json',
  },
};

export default function () {
  const res = http.post(`${BASE_URL}/rebac/check`, payload, params);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'decision is returned': (r) => r.json().allowed !== undefined,
  });
}
