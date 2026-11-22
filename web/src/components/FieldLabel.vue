<template>
  <span class="field-label">
    <span class="field-label-main">
      {{ parts.main }}<span v-if="tag" class="field-tag" :class="`field-tag--${tone}`" :data-tag="tag">{{ tag }}</span>
    </span>
    <span v-if="parts.note" class="label-note">{{ parts.note }}</span>
  </span>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const props = withDefaults(defineProps<{
  text: string
  /** 实测结论标签（推荐/中性偏负/负向/维持等），文字区分不依赖颜色 */
  tag?: string
  tone?: 'good' | 'warn' | 'bad' | 'neutral'
}>(), { tag: '', tone: 'neutral' })

/** 把「主标题（注释）」拆成主标题与独立第二行；无括号时原样显示。 */
const parts = computed(() => {
  const m = /^([^（(]+)[（(](.+)[）)]$/.exec(props.text || '')
  return m ? { main: m[1], note: `（${m[2]}）` } : { main: props.text || '', note: '' }
})
</script>
