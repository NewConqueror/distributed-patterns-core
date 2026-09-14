import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

// Custom Metrics
export const reservedOrders = new Counter('reserved_orders');
export const outOfStockOrders = new Counter('out_of_stock_orders');
export const failedOrders = new Counter('failed_orders');

export const options = {
  scenarios: {
    fast_burst: {
      executor: 'shared-iterations',
      vus: 100,             // 100 Virtual Users
      iterations: 2000,     // 2,000 total requests
      maxDuration: '2s',    // Within 2 seconds burst window
    },
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:8080';
const ENDPOINT = `${BASE_URL}/api/v1/orders/fast`;

export default function () {
  const payload = JSON.stringify({
    product_id: 1,
    user_id: `user_${__VU}_${__ITER}`,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(ENDPOINT, payload, params);

  if (res.status === 202) {
    // Reservation accepted in Redis
    reservedOrders.add(1);
  } else if (res.status === 410 || res.status === 400) {
    // Sold out fast-fail (Zero DB hits!)
    outOfStockOrders.add(1);
  } else {
    failedOrders.add(1);
  }

  check(res, {
    'status is 202 (accepted)': (r) => r.status === 202,
    'status is 410 (out of stock)': (r) => r.status === 410,
    'status is 500 (unexpected error)': (r) => r.status === 500,
  });
}
