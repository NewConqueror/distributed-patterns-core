import http from 'k6/http';
import { check, sleep } from 'k6';

// Scenario 2: Warm Cache (L1 In-Memory + L2 Distributed Redis)
// Objective: Prove multi-tier caching efficiency.
// Target: p99 latency < 3ms under extreme concurrency.

export const options = {
  scenarios: {
    warm_cache_concurrency: {
      executor: 'ramping-vus',
      startVUs: 50,
      stages: [
        { duration: '10s', target: 500 },  // Ramp to 500 concurrent VUs
        { duration: '20s', target: 2000 }, // Peak load with 2,000 VUs
        { duration: '10s', target: 0 },    // Ramp-down
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.001'],       // Less than 0.1% failure
    http_req_duration: ['p(99)<3'],        // Gold Standard: p99 < 3ms
    'http_req_duration{status:200}': ['p(95)<1'], // p95 sub-millisecond on L1 hits
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:8080';
const DEFAULT_TENANT_ID = '00000000-0000-0000-0000-000000000001';

export default function () {
  // Frequently requested hot keys to exercise L1 LRU and L2 Redis hits
  const hotKeys = [
    { sub: 'user:u-1-admin', obj: 'document:doc-1-l5-1', perm: 'viewer' },
    { sub: 'user:u-1-m-1',   obj: 'document:doc-1-l5-2', perm: 'viewer' },
    { sub: 'user:u-2-admin', obj: 'document:doc-2-l5-1', perm: 'viewer' },
    { sub: 'user:u-3-m-1',   obj: 'document:doc-3-l5-1', perm: 'viewer' },
  ];

  const selected = hotKeys[Math.floor(Math.random() * hotKeys.length)];

  const payload = JSON.stringify({
    tenant_id: DEFAULT_TENANT_ID,
    subject: selected.sub,
    permission: selected.perm,
    object: selected.obj,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(`${BASE_URL}/rebac/check`, payload, params);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'allowed decision returned': (r) => r.json().allowed !== undefined,
  });

  sleep(0.001); // Minimal sleep for high-throughput concurrency
}
