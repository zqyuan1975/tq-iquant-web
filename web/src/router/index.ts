import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', redirect: '/dashboard' },
    { path: '/dashboard', name: 'dashboard', component: () => import('../views/Dashboard.vue') },
    { path: '/stock-pools', name: 'stock-pools', component: () => import('../views/StockPools.vue') },
    { path: '/investment-board', name: 'investment-board', component: () => import('../views/InvestmentBoard.vue') },
    { path: '/formulas', name: 'formulas', component: () => import('../views/Formulas.vue') },
    { path: '/portfolios', name: 'portfolios', component: () => import('../views/Portfolios.vue') },
    { path: '/backtest', name: 'backtest', component: () => import('../views/Backtest.vue') },
    { path: '/backtest/analysis', name: 'backtest-analysis', component: () => import('../views/BacktestAnalysis.vue') },
    { path: '/incremental', name: 'incremental', component: () => import('../views/IncrementalBacktest.vue') },
    { path: '/live', name: 'live', component: () => import('../views/LiveSessions.vue') },
    { path: '/health', name: 'health', component: () => import('../views/HealthOverview.vue') },
    { path: '/cash-outlets', name: 'cash-outlets', component: () => import('../views/CashOutlets.vue') },
    { path: '/events', name: 'events', component: () => import('../views/EventSearch.vue') },
    { path: '/troubleshooting', name: 'troubleshooting', component: () => import('../views/Troubleshooting.vue') },
    { path: '/system', name: 'system', component: () => import('../views/SystemConfig.vue') },
    { path: '/guide', name: 'guide', component: () => import('../views/Guide.vue') },
  ],
})

export default router
