import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/events':      'http://localhost:8080',
      '/admissions':  'http://localhost:8080',
      '/discharges':  'http://localhost:8080',
      '/transfers':   'http://localhost:8080',
      '/alerts':      'http://localhost:8080',
      '/lab-results': 'http://localhost:8080',
      '/simulator':   'http://localhost:8080',
      '/chaos':       'http://localhost:8080',
      '/health':      'http://localhost:8080',
      '/metrics':     'http://localhost:8080',
    }
  }
})
