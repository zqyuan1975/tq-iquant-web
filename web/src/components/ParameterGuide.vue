<template>
  <div class="param-guide" data-component="parameter-guide">
    <p class="param-guide-intro">
      这份指南讲「怎么选、为什么」：每项给出建议值、实测依据（含回测记录 id）与不适用场景。
      字段定义、取值范围和继承细节请看「配置指南」，两边不重复。
      数字均来自同一区间 2021-01-04 → 2026-11-19 的消融与容量核查，记录 id 对应
      <code>backtest_records</code>。
    </p>
    <details v-for="grp in GROUPS" :key="grp.key" class="param-guide-group">
      <summary>
        <span class="param-guide-group-title">{{ grp.title }}</span>
        <span class="param-guide-group-hint">{{ grp.hint }}</span>
      </summary>
      <div class="param-guide-items">
        <div v-for="item in grp.items" :key="item.key" class="param-guide-item">
          <p class="param-guide-name">
            {{ item.name }}
            <code>{{ item.key }}</code>
          </p>
          <p class="param-guide-line"><span class="param-guide-tag">建议值</span><span>{{ item.value }}</span></p>
          <p class="param-guide-line"><span class="param-guide-tag">为什么</span><span>{{ item.why }}</span></p>
          <p class="param-guide-line"><span class="param-guide-tag">什么时候别用</span><span>{{ item.avoid }}</span></p>
          <p class="param-guide-line"><span class="param-guide-tag">继承</span><span>{{ item.inherit }}</span></p>
        </div>
      </div>
    </details>
  </div>
</template>

<script setup lang="ts">
interface GuideItem {
  key: string
  name: string
  value: string
  why: string
  avoid: string
  inherit: string
}

interface GuideGroup {
  key: string
  title: string
  hint: string
  items: GuideItem[]
}

// 内容与 docs/plans/2026-11-22-成长组合参数设定定稿.md 保持一致，不另立数字。
const GROUPS: GuideGroup[] = [
  {
    key: 'gate',
    title: '门控',
    hint: '大盘多空 / 行业放行 / 跨策略排序 / 同行业上限',
    items: [
      {
        key: 'market_gate_enabled',
        name: '大盘多空门控',
        value: '开（本组合 true；全局默认 false）。',
        why: '门控本身未单独消融，但 A/B 的增益都在门控开启前提下测得：行业 relief 为 +799.8pp、回撤 −3.0pp（id 37 vs 36）。',
        avoid: '基准指数日线不足或长期横盘时空头段不新开仓，会错过反转首日；它只拦 OPEN/ADD，不会强平存量。',
        inherit: '双端继承，回测与实盘共用同一 tracker。',
      },
      {
        key: 'industry_gate_mode',
        name: '行业放行三档',
        value: 'relief（本组合）；全局仍保持 off。',
        why: 'relief 只在大盘空头时按行业放行多头，实测 +799.8pp、回撤 −3.0pp（id 37 vs 36）。',
        avoid: 'strict 常开、行业非多头一律拦，未做消融，修复期会大面积错过反弹；行业数据缺失时 fail-open（不拦也不保护）。',
        inherit: '双端继承，经 portfolio_settings.json 同时被回测与实盘读取。',
      },
      {
        key: 'industry_rank_enabled',
        name: '跨策略 OPEN 排序',
        value: '开。',
        why: 'B 实测 +323.1pp、回撤 −4.2pp（id 38 vs 37）。',
        avoid: '只有单一策略或单一持仓时排序无差异；行业分类缺失会退化为原顺序，看不到效果。',
        inherit: '双端继承。',
      },
      {
        key: 'industry_max_per_industry',
        name: '同行业持仓上限',
        value: '2。',
        why: '上限 1 为 −61.7pp（id 45 vs 38）、上限 3 为 −84.1pp（id 44 vs 38），2 最优。',
        avoid: '只在跨策略排序开启时生效；设得过小会掐掉强势行业的集中暴露，设 3 以上则基本退回未约束。',
        inherit: '双端继承。',
      },
    ],
  },
  {
    key: 'risk',
    title: '风控',
    hint: '止损 / 移动止损 / 行业转空收紧 / 空头减仓 / 超买停开仓',
    items: [
      {
        key: 'stop_loss_ratio',
        name: '止损',
        value: '5%。',
        why: '未单独消融；当前组合与全局默认都是 5%，定稿保持。',
        avoid: '过紧会被日内噪声打出，过松则单笔亏损失控；收盘触发、次日开盘成交，跳空时实际损失可能超过止损线。',
        inherit: '双端继承（策略字段）。',
      },
      {
        key: 'trailing_stop_ratio',
        name: '移动止损',
        value: '3%。',
        why: '未单独消融；它是成长组合的核心出场机制，移动止损实测贡献约 71.4% 的总利润（配置指南）。',
        avoid: '震荡市里 1%~2% 会被反复扫损；直接取消会让盈利回吐。',
        inherit: '双端继承（策略字段）。',
      },
      {
        key: 'industry_trail_tighten_enabled',
        name: '行业转空收紧止损',
        value: '关（false / 0.015）。',
        why: 'E 实测 −50.2pp、回撤反而 +2.0pp（id 43 vs 39）。',
        avoid: '行业转空时想更快落袋才用；当前证据表明它把趋势利润提前砍掉，回撤也没改善。',
        inherit: '双端继承。',
      },
      {
        key: 'industry_bear_reduce_ratio',
        name: '空头减仓',
        value: '0（关）；大盘空头档 market_gate_bear_reduce_ratio 同样保持 0。',
        why: 'F 实测 −108.1pp、回撤无改善（id 41 vs 40）。',
        avoid: '行业转空但个股仍强时会卖飞；它只与大盘空头档取 max、不叠加，未开大盘档时行业档也会独立减仓。',
        inherit: '双端继承。',
      },
      {
        key: 'industry_overbought_block',
        name: '超买停开仓',
        value: '关。',
        why: 'D 实测 −31.4pp（id 42 vs 39）。',
        avoid: 'z≥2.5 会拦掉主升段的新仓，成长股趋势延续时反而少赚。',
        inherit: '双端继承。',
      },
    ],
  },
  {
    key: 'capacity',
    title: '容量与流动性',
    hint: '单笔上限 / 参与率 / 容量模型 / 冲击系数',
    items: [
      {
        key: 'max_order_amount',
        name: '单笔上限',
        value: '1,000,000 元（组合当前 300,000）。',
        why: '30 万→100 万：总收益 +984.7pp、回撤仅 +0.95pp、Sharpe +0.31，触顶率 40.51%→0%（id 48 vs 38）；100 万与不限额逐位相同（id 50）；实测 ADV 中位 0.99 亿，100 万约占 1.01%。',
        avoid: 'ADV<0.5 亿的小盘（样本占 50.2%，中位 0.43 亿）单笔 100 万已超 2%，需按 min(100 万, 4%×ADV) 降到约 50 万；若放开 equity_cap，上限要按 ADV 重推。',
        inherit: '双端继承（策略字段，装配时两端读取）；run 级可临时覆盖、不落库。',
      },
      {
        key: 'volume_limit_ratio',
        name: '参与率（成交量占比）',
        value: '4%，并开启成交量约束。',
        why: '4% 同日量 + 线性冲击下 100 万单笔可执行；p10 标的 ADV 0.26 亿 × 4% ≈ 104 万，刚好覆盖。',
        avoid: '10% 只适合超大盘；对 ADV<0.5 亿标的必须切单，否则回测价失真。',
        inherit: '双端继承；注意回测默认 same_day_bar（当日 bar 量），实盘按前 5 日均量，同一标的同一天可成交量不同。',
      },
      {
        key: 'capacity_model',
        name: '容量模型',
        value: 'linear（保持默认）。',
        why: 'sqrt 四档逐位相同，收益仅 140.87%、回撤 21.22%（id 52–59），原因是 0.5%/1% 参与率先把订单切碎，净值停在 60 万级，口径过保守。',
        avoid: '确需机构级容量估算才用 sqrt，且必须同时把 k 从 0.10 降到 0.01–0.02，否则结果不能用于实盘预估。',
        inherit: 'linear 为组合默认、双端口径一致；sqrt 仅 run 级参数、不落库。',
      },
      {
        key: 'slippage_impact_coef',
        name: '冲击系数',
        value: '10%（线性口径保持 0.10）。',
        why: '线性冲击 = 20bps + 0.10 × 订单量/同 bar 量，1% 参与率约 30bps/边；100 万单笔冲击总额 23.5 万（id 48）。',
        avoid: 'sqrt 下冲击被强制开启，且 k=0.10 与 0.5%/1% 参与率不匹配（1% 被计 120bps），不能直接沿用。',
        inherit: '双端继承；run 级可临时覆盖。',
      },
    ],
  },
  {
    key: 'position',
    title: '单仓与组合',
    hint: '单票目标仓位 / 持仓数量 / 资金上限',
    items: [
      {
        key: 'single_open_ratio',
        name: '单票目标仓位',
        value: '20%（本组合；全局默认 10%）。',
        why: '与 capital_ratio=1.0、max_positions=5 共同决定单票 ≈20%×净值，5 仓接近满仓；与单笔 100 万、equity_cap 500 万对齐。',
        avoid: '单票 20% 没有现金缓冲，集体回撤时回撤更深；多策略共享资金时应按 capital_ratio 分摊。',
        inherit: '双端继承（策略字段）。',
      },
      {
        key: 'max_positions',
        name: '持仓数量',
        value: '策略 5 / 组合 20。',
        why: '当前策略最多 5 仓，组合 20 不会先触发，定稿保持。',
        avoid: '为提高分散度盲目调大 max_positions 会摊薄单票仓位、改变收益结构，必须重测。',
        inherit: '双端继承，策略与组合上限取更小约束。',
      },
      {
        key: 'equity_cap',
        name: '资金上限',
        value: '5,000,000 元。',
        why: '与单笔 100 万、单仓 20% 对齐（500 万 × 20% = 100 万），避免复利后单仓超过实盘可成交量。',
        avoid: '放开上限后单仓随净值放大，单笔上限要按 ADV 重推；实盘会话启动时只读一次，改完需重启会话才生效。',
        inherit: '双端继承；max_single_position_pct / min_cash_reserve_pct 仅实盘、回测不读。',
      },
    ],
  },
]
</script>
