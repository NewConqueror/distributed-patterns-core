import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const rateLimitedCount = new Counter('rate_limited_429_total');
const allowedCount = new Counter('allowed_200_total');
const rate429Ratio = new Rate('rate_limited_ratio');

export const options = {
  scenarios: {
    burst_traffic: {
      executor: 'constant-arrival-rate',
      rate: 200,
      timeUnit: '1s',
      duration: '5s',
      preAllocatedVUs: 20,
      maxVUs: 50,
    },
  },
  thresholds: {
    'rate_limited_429_total': ['count>10'], // Expect 429s during burst
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://127.0.0.1:8080';

function getHeader(headers, name) {
  for (let k in headers) {
    if (k.toLowerCase() === name.toLowerCase()) {
      return headers[k];
    }
  }
  return undefined;
}

export default function () {
  const payload = JSON.stringify({ item_id: 'prod-456', quantity: 1 });
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': 'client-burst-tester',
    },
  };

  const res = http.post(`${BASE_URL}/api/v1/checkout`, payload, params);

  if (res.status === 429) {
    rateLimitedCount.add(1);
    rate429Ratio.add(1);

    check(res, {
      'has Retry-After header': (r) => getHeader(r.headers, 'Retry-After') !== undefined,
      'has X-RateLimit-Remaining 0': (r) => getHeader(r.headers, 'X-RateLimit-Remaining') === '0',
      'body contains rate limit message': (r) => r.body.includes('Too Many Requests'),
    });
  } else if (res.status === 200) {
    allowedCount.add(1);
    rate429Ratio.add(0);

    check(res, {
      'has X-RateLimit-Limit header': (r) => getHeader(r.headers, 'X-RateLimit-Limit') !== undefined,
    });
  }
}
