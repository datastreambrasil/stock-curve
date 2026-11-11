import './style.css'
import { criarGrafico } from './chart'
import { fetchCurve, fetchTickers } from './api'
import type { QuotePoint, TickerSummary } from './api'

// O SPA inteiro: uma vez por segundo pergunta à API do consumidor quais papéis
// existem e qual é a curva do papel escolhido, e escreve o resultado nos
// elementos que já estão no index.html. A tela só lê — escolher o papel é a
// única interação.

const dinheiro = new Intl.NumberFormat('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
const inteiro = new Intl.NumberFormat('pt-BR')
const hora = new Intl.DateTimeFormat('pt-BR', { hour: '2-digit', minute: '2-digit', second: '2-digit' })

const elemento = (id: string) => document.getElementById(id)!
const grafico = criarGrafico(document.getElementById('chart') as HTMLCanvasElement)

let papel: string | null = null

// Um listener só para todos os botões de papel, inclusive os que ainda nem
// foram criados.
elemento('filters').addEventListener('click', (evento) => {
  const botao = (evento.target as HTMLElement).closest('button')
  if (!botao) return
  papel = botao.textContent
  void atualizar()
})

function mostrarPapeis(papeis: TickerSummary[]): void {
  elemento('filters').innerHTML = papeis
    .map((item) => `<button class="chip${item.ticker === papel ? ' on' : ''}">${item.ticker}</button>`)
    .join('')
}

function mostrarNumeros(resumo: TickerSummary | undefined, pontos: QuotePoint[]): void {
  if (!resumo || pontos.length === 0) {
    for (const id of ['price', 'high', 'low']) elemento(id).textContent = '—'
    for (const id of ['change', 'high-at', 'low-at', 'open']) elemento(id).textContent = ''
    elemento('points').textContent = '0'
    return
  }

  // Máxima e mínima saem dos próprios pontos da curva — a mesma janela que o
  // gráfico desenha.
  const maxima = pontos.reduce((a, b) => (b.price > a.price ? b : a))
  const minima = pontos.reduce((a, b) => (b.price < a.price ? b : a))

  elemento('price').textContent = `R$ ${dinheiro.format(resumo.price)}`
  elemento('change').textContent =
    `${resumo.changePct >= 0 ? '+' : ''}${dinheiro.format(resumo.changePct)}% na janela`
  elemento('change').className = `tile-note ${resumo.changePct < 0 ? 'down' : 'up'}`

  elemento('high').textContent = `R$ ${dinheiro.format(maxima.price)}`
  elemento('high-at').textContent = `às ${hora.format(new Date(maxima.tradedAt))}`

  elemento('low').textContent = `R$ ${dinheiro.format(minima.price)}`
  elemento('low-at').textContent = `às ${hora.format(new Date(minima.tradedAt))}`

  elemento('points').textContent = inteiro.format(resumo.points)
  elemento('open').textContent = `abertura R$ ${dinheiro.format(resumo.open)}`
}

async function atualizar(): Promise<void> {
  try {
    const papeis = await fetchTickers()
    elemento('alert').hidden = true

    if (papel === null || !papeis.some((item) => item.ticker === papel)) {
      papel = papeis[0]?.ticker ?? null
    }

    mostrarPapeis(papeis)
    elemento('chart-title').textContent = papel ? `Curva de preço · ${papel}` : 'Curva de preço'

    const pontos = papel ? (await fetchCurve(papel)).points : []
    mostrarNumeros(papeis.find((item) => item.ticker === papel), pontos)
    grafico.atualizar(pontos)
  } catch (erro) {
    elemento('alert').hidden = false
    elemento('alert').textContent = `Não consegui falar com a API do consumidor: ${(erro as Error).message}`
  }
}

void atualizar()
setInterval(() => void atualizar(), 1000)
