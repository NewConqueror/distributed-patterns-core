import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const fallbackP99Trend = new Trend('fallback_p99_latency');
const totalRequests = new Counter('total_chaos_requests');

export const options = {
  scenarios: {
    chaos_resilience: {
      executor: 'constant-arrival-rate',
      rate: 100, // 100 reqs/sec
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 150,
      maxVUs: 300,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'], // Gateway must recover gracefully via circuit breaker
    'fallback_p99_latency': ['p(99)<1000'], // p99 stays within 1s even with 30% upstream failure
  },
};

export default function () {
  const payload = JSON.stringify({
    model: 'gpt-4o',
    messages: [{ role: 'user', content: `Chaos resilient test prompt ${__ITER}` }],
    stream: true,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer k6-test-token',
    },
  };

  const start = Date.now();
  const res = http.post('http://localhost:8080/v1/chat/completions', payload, params);
  const elapsed = Date.now() - start;

  fallbackP99Trend.add(elapsed);
  totalRequests.add(1);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'stream completed successfully': (r) => r.body && r.body.includes('[DONE]'),
  });

  sleep(0.05);
}
