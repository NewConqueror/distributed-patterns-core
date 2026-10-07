import http from 'k6/http';
import { check, sleep } from 'k6';

// Scenario 1: Cold Cache (PostgreSQL RLS + Recursive CTE directly)
// Objective: Measure the maximum raw database throughput and recursive graph traversal latency.

export const options = {
  scenarios: {
    cold_db_benchmark: {
      executor: 'ramping-vus',
      startVUs: 5,
      stages: [
        { duration: '10s', target: 50 },  // Ramp-up to 50 concurrent VUs
        { duration: '20s', target: 100 }, // Sustained load on Postgres
        { duration: '10s', target: 0 },   // Ramp-down
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],         // Less than 1% error rate
    http_req_duration: ['p(95)<150'],       // 95% of requests under 150ms for deep recursive CTE
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:8080';

// Sample fallback test data if seed summary is not yet generated
const DEFAULT_TENANT_ID = '00000000-0000-0000-0000-000000000001';

export default function () {
  // Generate pseudo-random tenant and object IDs to avoid cache hits
  const randTenant = Math.floor(Math.random() * 100) + 1;
  const randDoc = Math.floor(Math.random() * 20) + 1;

  const payload = JSON.stringify({
    tenant_id: DEFAULT_TENANT_ID,
    subject: `user:u-${randTenant}-m-1`,
    permission: 'viewer',
    object: `document:doc-${randTenant}-l5-${randDoc}`,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-Bypass-Cache': 'true', // Bypasses L1 and L2 caches, forcing PostgreSQL RLS + CTE
    },
  };

  const res = http.post(`${BASE_URL}/rebac/check`, payload, params);

  check(res, {
    'status is 200': (r) => r.status === 200,
  });

  sleep(0.01);
}
