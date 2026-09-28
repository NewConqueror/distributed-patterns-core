import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const cacheHitDuration = new Trend('cache_hit_duration');
const cacheHitCounter = new Counter('cache_hits');
const cacheMissCounter = new Counter('cache_misses');

export const options = {
  scenarios: {
    mixed_cache_load: {
      executor: 'ramping-vus',
      startVUs: 100,
      stages: [
        { duration: '15s', target: 500 },
        { duration: '30s', target: 1000 },
        { duration: '15s', target: 0 },
      ],
    },
  },
  thresholds: {
    cache_hit_duration: ['p(95)<50'], // Cache hits must return in <50ms
    http_req_failed: ['rate<0.01'],
  },
};

const cachedPool = [
  'What is the capital of France?',
  'How does Go garbage collection work?',
  'Explain Dijkstra shortest path algorithm',
  'What is the difference between TCP and UDP?',
  'Write a quicksort implementation in Python',
];

export default function () {
  const isHit = Math.random() < 0.50; // 50% hit target
  let prompt = '';

  if (isHit) {
    // Select from cached pool with minor whitespace variations to test semantic matching
    const base = cachedPool[Math.floor(Math.random() * cachedPool.length)];
    prompt = `  ${base}   `;
  } else {
    prompt = `Completely unique query ${__VU}-${__ITER}-${Math.random()}`;
  }

  const payload = JSON.stringify({
    model: 'gpt-4o',
    messages: [{ role: 'user', content: prompt }],
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

  if (res.headers['X-Cache'] === 'HIT') {
    cacheHitCounter.add(1);
    cacheHitDuration.add(elapsed);
  } else {
    cacheMissCounter.add(1);
  }

  check(res, {
    'status is 200': (r) => r.status === 200,
    'cache hit under 50ms': (r) => r.headers['X-Cache'] !== 'HIT' || elapsed < 50,
  });

  sleep(0.02);
}
