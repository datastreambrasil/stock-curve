import { Chart } from 'chart.js/auto'
import type { QuotePoint } from './api'

// O gráfico é o Chart.js com a configuração mínima: uma série, sem legenda
// (o título do card já diz qual papel está na tela) e sem animação, porque a
// curva é redesenhada a cada segundo.
//
// As cores não ficam aqui: são lidas do CSS (as variáveis de style.css), para
// haver um só lugar para mexer na aparência da tela inteira.

const hora = new Intl.DateTimeFormat('pt-BR', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
const dinheiro = new Intl.NumberFormat('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
const inteiro = new Intl.NumberFormat('pt-BR')

function cor(nome: string): string {
  return getComputedStyle(document.body).getPropertyValue(nome).trim()
}

export function criarGrafico(canvas: HTMLCanvasElement) {
  // Os pontos originais ficam aqui ao lado: o Chart.js só recebe os preços, e
  // o tooltip volta nesta lista para achar a partição e o offset.
  let pontos: QuotePoint[] = []

  const grafico = new Chart(canvas, {
    type: 'line',
    data: {
      labels: [] as string[],
      datasets: [{
        data: [] as number[],
        borderWidth: 2,
        pointRadius: 0,
        pointHoverRadius: 4,
        fill: true,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      // Com "index" o tooltip aparece ao passar o mouse em qualquer altura da
      // coluna, não só exatamente em cima do ponto.
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (item) => `R$ ${dinheiro.format(item.parsed.y ?? 0)}`,
            // De onde veio este ponto no log — é o que liga o gráfico ao Kafka.
            afterBody: (itens) => {
              const ponto = pontos[itens[0].dataIndex]
              return `volume ${inteiro.format(ponto.volume)}\npartição ${ponto.partition} · offset ${ponto.offset}`
            },
          },
        },
      },
      scales: {
        x: { ticks: { maxTicksLimit: 6, maxRotation: 0 } },
        y: { ticks: { maxTicksLimit: 6, callback: (valor) => dinheiro.format(Number(valor)) } },
      },
    },
  })

  // Lê as cores do CSS e joga no Chart.js.
  function aplicarCores() {
    const serie = cor('--series')
    const dataset = grafico.data.datasets[0]
    dataset.borderColor = serie
    dataset.backgroundColor = cor('--series-wash')
    dataset.pointHoverBackgroundColor = serie
    dataset.pointHoverBorderColor = cor('--surface')
    dataset.pointHoverBorderWidth = 2

    for (const eixo of [grafico.options.scales!.x!, grafico.options.scales!.y!]) {
      eixo.ticks!.color = cor('--muted')
      eixo.grid!.color = cor('--grid')
    }

    grafico.update()
  }

  aplicarCores()

  return {
    atualizar(novos: QuotePoint[]) {
      pontos = novos
      grafico.data.labels = pontos.map((ponto) => hora.format(new Date(ponto.tradedAt)))
      grafico.data.datasets[0].data = pontos.map((ponto) => ponto.price)
      grafico.update()
    },
  }
}
