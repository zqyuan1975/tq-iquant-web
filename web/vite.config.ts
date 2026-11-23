import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  server: {
    // 使用说明直接以仓库根 docs/*.md 为源导入，dev server 需要放行上级目录
    fs: { allow: ['..'] },
    proxy: {
      '/api': 'http://localhost:8001',
    },
  },
  build: {
    outDir: 'dist',
  },
})
