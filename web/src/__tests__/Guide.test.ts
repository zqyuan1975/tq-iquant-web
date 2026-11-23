import { mount } from '@vue/test-utils'
import { nextTick } from 'vue'
import { describe, expect, it } from 'vitest'
import Guide from '../views/Guide.vue'

/** 使用说明页直接渲染 docs/用户使用说明.md：校验标题/目录锚点/表格/代码块都在。 */
describe('Guide', () => {
  it('renders markdown source with anchors, tables and code blocks', async () => {
    const wrapper = mount(Guide, { global: { stubs: { RouterLink: true } } })
    await nextTick()
    await nextTick()

    expect(wrapper.find('.guide-md h1').text()).toContain('使用说明')

    // 目录已移到右侧悬浮栏：正文不再保留「目录」章节，侧栏链接命中同名标题 id
    const tocLinks = wrapper.findAll('.guide-toc a')
    expect(tocLinks.length).toBe(12)
    expect(wrapper.findAll('.guide-md h2').some((h) => h.text() === '目录')).toBe(false)
    const hrefs = tocLinks.map((a) => decodeURIComponent(a.attributes('href') || ''))
    expect(hrefs).toContain('#一首次启动')
    expect(wrapper.find('#一首次启动').exists()).toBe(true)
    expect(tocLinks[0].text()).toContain('首次启动')

    expect(wrapper.findAll('.md-table-wrap > table').length).toBeGreaterThan(0)
    expect(wrapper.findAll('pre > code').length).toBeGreaterThan(0)
    expect(wrapper.findAll('.guide-md li').length).toBeGreaterThan(10)
  })
})
