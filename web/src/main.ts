import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import { initTheme } from './composables/useTheme'
import './style.css'

// 初始化主题（在 mount 前设置 data-theme，避免闪烁）；解析规则与切换逻辑同一处，避免两套口径
initTheme()

createApp(App).use(router).mount('#app')
