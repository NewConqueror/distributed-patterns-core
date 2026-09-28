import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// Custom metrics matching our Prometheus metrics
const ttftTrend = new Trend('llm_time_to_first_token');
const cacheMissCounter = new Counter('cache_misses');

export const options = {
  scenarios: {
    cache_miss_load: {
      executor: 'constant-vus',
      vus: 200,
      duration: '30s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'], // <1% errors allowed
    http_req_duration: ['p(95)<500'], // 95% of requests under 500ms
  },
};

export default function () {
  const uniquePrompt = `What is quantum computing concept number ${__VU}-${__ITER}-${Date.now()}?`;

  const payload = JSON.stringify({
    model: 'gpt-4o',
    messages: [{ role: 'user', content: uniquePrompt }],
    stream: true,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer k6-test-token',
    },
    timeout: '10s',
  };

  const start = Date.now();
  const res = http.post('http://localhost:8080/v1/chat/completions', payload, params);
  const elapsed = Date.now() - start;

  ttftTrend.add(elapsed);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'is cache MISS': (r) => r.headers['X-Cache'] === 'MISS',
    'has SSE content': (r) => r.body && r.body.includes('data:'),
  });

  if (res.headers['X-Cache'] === 'MISS') {
    cacheMissCounter.add(1);
  }

  sleep(0.05);
}
