import http from 'k6/http';
import { check } from 'k6';

// Cada VU fica chamando POST /quotes, e cada chamada publica uma cotação no
// Kafka. O número que interessa no final é o http_reqs/s: é o throughput do
// produtor. Mexa em LingerMs/CompressionType no compose e rode de novo.

const BASE_URL = __ENV.PRODUCER_URL || 'http://localhost:5001';

export const options = {
  stages: [
    { duration: '10s', target: 20 },  // aquecimento
    { duration: '30s', target: 100 }, // carga
    { duration: '10s', target: 0 },   // desaceleração
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<200'],
  },
};

export default function () {
  const res = http.post(`${BASE_URL}/quotes`);

  check(res, {
    'status 200': (r) => r.status === 200,
    'veio partição e offset': (r) => r.json('offset') !== undefined,
  });
}
