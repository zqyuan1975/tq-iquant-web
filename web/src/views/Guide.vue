<script setup lang="ts">
import { nextTick, onMounted, onUnmounted, ref } from 'vue'
import { marked } from 'marked'
// 单一来源：构建时直接把仓库根目录的使用说明打进产物，不维护第二份副本。
import guideSource from '../../../docs/用户使用说明.md?raw'

marked.setOptions({ gfm: true, breaks: false })

/**
 * 标题锚点：文档目录里的链接（如 #一首次启动）用的是 GitHub 风格中文 slug，
 * 这里按同一规则从标题文本生成 id，保证目录可跳转。
 */
function slugify(text: string): string {
  return text
    .trim()
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s_-]/gu, '')
    .trim()
    .replace(/\s+/g, '-')
}

interface TocItem { id: string; num: string; title: string }

const html = marked.parse(guideSource) as string
const bodyRef = ref<HTMLElement | null>(null)
/** 侧栏目录：章节 id + 中文序号 + 标题（序号单独着色，窄栏更省宽度） */
const toc = ref<TocItem[]>([])
/** 当前阅读到的章节，用于侧栏目录高亮 */
const activeId = ref('')

let scrollEl: HTMLElement | null = null
let rafId = 0

function updateActive() {
  if (!scrollEl) return
  const line = scrollEl.getBoundingClientRect().top + 32
  let current = ''
  for (const item of toc.value) {
    const el = document.getElementById(item.id)
    if (!el) continue
    if (el.getBoundingClientRect().top <= line) current = item.id
    else break
  }
  activeId.value = current
}

function onScroll() {
  if (rafId) return
  rafId = window.requestAnimationFrame(() => {
    rafId = 0
    updateActive()
  })
}

/** 目录跳转：手动滚动避免触发路由 hash 跳转，同时立即高亮目标章节 */
function goSection(e: MouseEvent, id: string) {
  const el = document.getElementById(id)
  if (!el) return
  e.preventDefault()
  el.scrollIntoView({ behavior: 'smooth', block: 'start' })
  activeId.value = id
  history.replaceState(null, '', `#${id}`)
}

onMounted(async () => {
  await nextTick()
  const root = bodyRef.value
  if (!root) return

  const used = new Map<string, number>()
  root.querySelectorAll<HTMLElement>('h1,h2,h3,h4,h5,h6').forEach((h) => {
    const base = slugify(h.textContent || '')
    if (!base) return
    const n = used.get(base) ?? 0
    used.set(base, n + 1)
    h.id = n === 0 ? base : `${base}-${n}`
  })

  // 表格套一层横向滚动容器：窄屏只在表内滚动，整页不横向溢出。
  root.querySelectorAll('table').forEach((table) => {
    if (table.parentElement?.classList.contains('md-table-wrap')) return
    const wrap = document.createElement('div')
    wrap.className = 'md-table-wrap'
    table.replaceWith(wrap)
    wrap.appendChild(table)
  })

  // 正文里的「目录」章节移到右侧悬浮目录后，从流式正文中移除（含多余分隔线）
  const tocHeading = Array.from(root.querySelectorAll('h2')).find((h) => (h.textContent || '').trim() === '目录')
  if (tocHeading) {
    const list = tocHeading.nextElementSibling
    if (list && list.tagName === 'OL') list.remove()
    const hr = tocHeading.nextElementSibling
    if (hr && hr.tagName === 'HR') hr.remove()
    tocHeading.remove()
  }

  toc.value = Array.from(root.querySelectorAll('h2'))
    .filter((h) => h.id)
    .map((h) => {
      const text = (h.textContent || '').trim()
      const m = text.match(/^([一二三四五六七八九十百]+)[、.．]\s*(.*)$/)
      return { id: h.id, num: m ? m[1] : '', title: m ? m[2] : text }
    })

  scrollEl = root.closest('.content') as HTMLElement | null
  scrollEl?.addEventListener('scroll', onScroll, { passive: true })
  updateActive()
})

onUnmounted(() => {
  scrollEl?.removeEventListener('scroll', onScroll)
  if (rafId) window.cancelAnimationFrame(rafId)
  scrollEl = null
})

function backToTop() {
  bodyRef.value?.closest('.content')?.scrollTo({ top: 0, behavior: 'smooth' })
}
</script>

<template>
  <div class="guide-page">
    <div class="guide-bar">
      <router-link class="btn btn-sm" to="/dashboard">← 返回总览</router-link>
      <button class="btn btn-sm" @click="backToTop">回到顶部</button>
    </div>

    <!-- 窄屏放不下侧栏时，目录折叠在正文上方，展开后向下推开正文而不是盖住 -->
    <details class="guide-toc-inline">
      <summary>目录（{{ toc.length }} 章）</summary>
      <ol>
        <li v-for="t in toc" :key="t.id">
          <a :href="`#${t.id}`" @click="goSection($event, t.id)">{{ t.title }}</a>
        </li>
      </ol>
    </details>

    <div class="guide-layout">
      <article ref="bodyRef" class="guide-md" v-html="html" />
      <aside class="guide-toc" aria-label="文档目录">
        <div class="guide-toc-title">目录</div>
        <nav>
          <a
            v-for="t in toc"
            :key="t.id"
            :href="`#${t.id}`"
            :class="{ active: activeId === t.id }"
            :aria-current="activeId === t.id ? 'true' : undefined"
            @click="goSection($event, t.id)"
          >
            <span v-if="t.num" class="guide-toc-num">{{ t.num }}</span>
            <span class="guide-toc-text">{{ t.title }}</span>
          </a>
        </nav>
      </aside>
    </div>
  </div>
</template>

<style>
.guide-page { max-width: 1240px; margin: 0 auto; }
.guide-bar { display: flex; justify-content: space-between; gap: 10px; margin-bottom: 14px; }
/* 正文 + 右侧悬浮目录：目录粘在滚动容器里，长文滚动时始终可见 */
.guide-layout { display: grid; grid-template-columns: minmax(0, 1fr) 236px; gap: 22px; align-items: start; }
.guide-toc {
  position: sticky;
  top: 12px;
  align-self: start;
  max-height: calc(100vh - 150px);
  overflow-y: auto;
  padding: 12px 8px 12px 14px;
  border: 1px solid var(--border);
  border-radius: var(--radius);
  background: var(--card-bg);
  box-shadow: var(--shadow-card);
  font-size: 12.5px;
}
.guide-toc-title {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: .16em;
  color: var(--ink-faint);
  padding: 0 8px 8px;
  border-bottom: 1px solid var(--line);
  margin-bottom: 6px;
}
.guide-toc nav { display: flex; flex-direction: column; }
.guide-toc a {
  display: flex;
  gap: 6px;
  align-items: baseline;
  padding: 5px 8px;
  border-left: 2px solid transparent;
  color: var(--ink-soft);
  line-height: 1.45;
  text-decoration: none;
}
.guide-toc a:hover { color: var(--ink); background: var(--hover-bg); }
.guide-toc a.active { color: var(--ink); border-left-color: var(--red); background: var(--accent-light); font-weight: 600; }
.guide-toc-num { font-family: var(--font-serif); color: var(--red); flex: 0 0 auto; }
.guide-toc-text { min-width: 0; }
/* 窄屏（<1200px）侧栏放不下：目录折叠在正文上方，展开时推开正文，不做浮层遮挡 */
.guide-toc-inline {
  display: none;
  margin-bottom: 12px;
  padding: 10px 14px;
  border: 1px solid var(--border);
  border-radius: var(--radius);
  background: var(--card-bg);
  box-shadow: var(--shadow-card);
}
.guide-toc-inline summary { cursor: pointer; font-size: 13px; font-weight: 700; color: var(--ink); }
.guide-toc-inline ol { margin: 10px 0 2px; padding-left: 20px; }
.guide-toc-inline li { margin: 4px 0; }
.guide-toc-inline a { color: var(--ink-soft); text-decoration: none; }
.guide-toc-inline a:hover { color: var(--link); text-decoration: underline; }
@media (max-width: 1199px) {
  .guide-layout { grid-template-columns: minmax(0, 1fr); }
  .guide-toc { display: none; }
  .guide-toc-inline { display: block; }
}
.guide-md {
  background: var(--card-bg);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  box-shadow: var(--shadow-card);
  padding: 28px 32px 40px;
  color: var(--ink);
  font-size: 14.5px;
  line-height: 1.75;
  overflow-wrap: break-word;
  word-break: break-word;
}
.guide-md > :first-child { margin-top: 0; }
.guide-md h1, .guide-md h2, .guide-md h3, .guide-md h4, .guide-md h5, .guide-md h6 {
  color: var(--ink);
  font-weight: 700;
  line-height: 1.35;
  scroll-margin-top: 16px;
}
.guide-md h1 {
  font-family: var(--font-serif);
  font-size: 27px;
  letter-spacing: .01em;
  margin: 0 0 18px;
  padding-bottom: 12px;
  border-bottom: 2px solid var(--line-strong);
}
.guide-md h2 {
  font-family: var(--font-serif);
  font-size: 21px;
  margin: 34px 0 14px;
  padding-left: 10px;
  border-left: 3px solid var(--red);
}
.guide-md h3 { font-size: 16.5px; margin: 24px 0 10px; color: var(--ink); }
.guide-md h4 { font-size: 15px; margin: 18px 0 8px; color: var(--ink-soft); }
.guide-md h5, .guide-md h6 { font-size: 14px; margin: 16px 0 8px; color: var(--ink-soft); }
.guide-md p { margin: 10px 0; }
.guide-md strong { font-weight: 700; color: var(--ink); }
.guide-md ul, .guide-md ol { margin: 10px 0; padding-left: 24px; }
.guide-md li { margin: 4px 0; }
.guide-md li > ul, .guide-md li > ol { margin: 4px 0; }
.guide-md hr { border: 0; border-top: 1px solid var(--line); margin: 26px 0; }
.guide-md a { color: var(--link); text-decoration: underline; text-underline-offset: 3px; }
.guide-md a:hover { color: var(--link-hover); }
.guide-md blockquote {
  margin: 14px 0;
  padding: 10px 14px;
  border-left: 3px solid var(--accent);
  background: var(--accent-light);
  color: var(--ink-soft);
}
.guide-md blockquote p { margin: 4px 0; }
.guide-md code {
  font-family: var(--font-mono);
  font-size: 12.5px;
  padding: 1px 5px;
  border-radius: var(--radius);
  background: var(--hover-bg);
  border: 1px solid var(--line);
  overflow-wrap: anywhere;
}
.guide-md pre {
  margin: 12px 0;
  padding: 14px 16px;
  overflow-x: auto;
  background: var(--bg);
  border: 1px solid var(--line);
  border-radius: var(--radius);
  line-height: 1.6;
}
.guide-md pre code {
  padding: 0;
  border: 0;
  background: transparent;
  font-size: 12.5px;
  white-space: pre;
  overflow-wrap: normal;
}
.guide-md .md-table-wrap {
  margin: 14px 0;
  overflow-x: auto;
  border: 1px solid var(--line);
  border-radius: var(--radius);
}
.guide-md table {
  width: 100%;
  min-width: 560px;
  border-collapse: collapse;
  font-size: 13px;
}
.guide-md th, .guide-md td {
  padding: 8px 12px;
  border-bottom: 1px solid var(--line);
  border-right: 1px solid var(--line);
  text-align: left;
  vertical-align: top;
  /* 单元格单行显示：长文案交给外层 .md-table-wrap 横向滚动，不在格内折行 */
  white-space: nowrap;
}
.guide-md th:last-child, .guide-md td:last-child { border-right: 0; }
.guide-md tr:last-child td { border-bottom: 0; }
.guide-md th { background: var(--hover-bg); font-weight: 700; white-space: nowrap; }
.guide-md img { max-width: 100%; height: auto; }

@media (max-width: 760px) {
  .guide-md { padding: 18px 14px 28px; font-size: 14px; }
  .guide-md h1 { font-size: 22px; }
  .guide-md h2 { font-size: 18px; }
}
</style>
