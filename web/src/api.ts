// Tudo que o SPA sabe sobre o consumidor passa por aqui. O prefixo /api é
// resolvido pelo proxy do Vite (dev) ou pelo nginx (Docker).

export interface QuotePoint {
  price: number
  volume: number
  tradedAt: string
  partition: number
  offset: number
}

export interface TickerSummary {
  ticker: string
  price: number
  open: number
  changePct: number
  points: number
  updatedAt: string
}

async function get<T>(path: string): Promise<T> {
  const response = await fetch(`/api${path}`)
  if (!response.ok) {
    throw new Error(`${path} respondeu ${response.status}`)
  }
  return response.json() as Promise<T>
}

export const fetchTickers = () => get<TickerSummary[]>('/tickers')

export const fetchCurve = (ticker: string) =>
  get<{ ticker: string; points: QuotePoint[] }>(`/quotes/${ticker}?limit=300`)
