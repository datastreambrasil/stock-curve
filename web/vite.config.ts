import { defineConfig } from 'vite'

// Em desenvolvimento o SPA fala com a API do consumidor pelo prefixo /api,
// que o Vite redireciona para localhost:5002. No Docker quem faz esse papel é
// o nginx (web/nginx.conf), então o código do app não muda entre os dois.
export default defineConfig({
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:5002',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
})
