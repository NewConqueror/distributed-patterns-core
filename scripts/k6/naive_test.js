import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

// Custom Metrics
export const successfulOrders = new Counter('successful_orders');
export const failedOrders = new Counter('failed_orders');
export const outOfStockOrders = new Counter('out_of_stock_orders');

export const options = {
  scenarios: {
    naive_burst: {
      executor: 'shared-iterations',
      vus: 100,             // 100 Virtual Users
      iterations: 2000,     // 2,000 total requests
      maxDuration: '2s',    // Within 2 seconds burst
    },
  },
};

const BASE_URL = __ENV.API_URL || 'http://localhost:8080';
const ENDPOINT = `${BASE_URL}/api/v1/orders/naive`;

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

  if (res.status === 201) {
    successfulOrders.add(1);
  } else if (res.status === 400) {
    outOfStockOrders.add(1);
  } else {
    // 500 DB Check Constraint violation or Connection Pool exhaustion
    failedOrders.add(1);
  }

  check(res, {
    'status is 201 (success)': (r) => r.status === 201,
    'status is 400 (out of stock)': (r) => r.status === 400,
    'status is 500 (db constraint/pool error)': (r) => r.status === 500,
  });
}
