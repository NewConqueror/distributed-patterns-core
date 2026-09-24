import http from 'k6/http';
import { check, sleep } from 'k6';

// k6 Load & Stress Test Yapılandırması: 1000 VUs (Sanal Kullanıcı)
export const options = {
  scenarios: {
    stress_test: {
      executor: 'ramping-vus',
      startVUs: 10,
      stages: [
        { duration: '3s', target: 200 },   // 3 saniyede 200 VU'ya çık
        { duration: '5s', target: 1000 },  // 5 saniyede 1000 VU zirvesine ulaş
        { duration: '10s', target: 1000 }, // 10 saniye boyunca 1000 VU ile stres altında tut
        { duration: '3s', target: 0 },     // 3 saniyede soğuma (ramp down)
      ],
      gracefulRampDown: '2s',
    },
  },
  thresholds: {
    // 1000 VU altında tek makinede p95 < 1500ms, p99 < 2000ms eşikleri
    http_req_duration: ['p(95)<1500', 'p(99)<2000'],
    http_req_failed: ['rate<0.01'], // Hata oranı <%1 olmalı
  },
};

const BASE_URL = 'http://localhost:8080';

// Basit rastgele UUID v4 üretici
function generateUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export default function () {
  const vuId = __VU;
  const iterId = __ITER;
  const idempKey = generateUUID();
  const txId = `tx-k6-vu${vuId}-it${iterId}-${Date.now()}`;

  // 1 ila 50 arasındaki varlık hesapları ile 51 ila 100 arasındaki yükümlülük hesapları arasında transfer
  const fromNum = (vuId % 50) + 1;
  const toNum = (vuId % 50) + 51;
  const fromAcc = `acc-load-${String(fromNum).padStart(3, '0')}`;
  const toAcc = `acc-load-${String(toNum).padStart(3, '0')}`;

  const payload = JSON.stringify({
    transaction_id: txId,
    description: `k6 yük testi transferi VU ${vuId}`,
    currency: 'USD',
    entries: [
      { account_id: fromAcc, direction: 'DEBIT', amount: 100 },
      { account_id: toAcc, direction: 'CREDIT', amount: 100 },
    ],
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': idempKey,
    },
    timeout: '5s',
  };

  const res = http.post(`${BASE_URL}/transactions`, payload, params);

  check(res, {
    'status is 201': (r) => r.status === 201,
    'status is valid': (r) => r.status === 200 || r.status === 201,
  });

  // Her 50 istekte bir bağlantı havuzu durumunu (connection pool) sorgula
  if (iterId % 50 === 0) {
    const metricsRes = http.get(`${BASE_URL}/metrics`);
    check(metricsRes, {
      'metrics status 200': (r) => r.status === 200,
    });
  }

  sleep(0.01); // 10ms bekleme
}
