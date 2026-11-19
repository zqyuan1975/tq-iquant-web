/**
 * 人工审批队列接口（2026-11-19）。
 *
 * 复用后端既有 `live_approval_queue` 与
 * `GET/POST /api/live/sessions/{id}/approvals[...]`，不新增路由或表。
 */
import { api } from './index'

export interface LiveApprovalItem {
  id: number
  portfolio_strategy_id: number
  strategy_id: number
  stock_code: string
  trade_type: string
  price: number | null
  quantity: number
  signal_type: string | null
  signal_name: string | null
  period: string | null
  bar_time: string | null
  signal_bar_date: string | null
  status: string
  reason: string | null
  created_at: string | null
  expires_at: string | null
  /** 后端补序列化字段：策略名 / 组合名 / 信号价×数量 / 是否已过期。 */
  strategy_name?: string | null
  portfolio_name?: string | null
  amount?: number | null
  expired?: boolean
}

export interface LiveApprovalList {
  manual_mode: boolean
  approvals: LiveApprovalItem[]
}

export interface LiveApprovalDecision {
  status: string
  approval_id: number
  live_order_id?: number | null
  reason?: string | null
}

export async function getLiveApprovals(sessionId: number): Promise<LiveApprovalList> {
  const { data } = await api.get<{ code: number; data: LiveApprovalList }>(
    `/live/sessions/${sessionId}/approvals`,
  )
  return data.data
}

export async function approveLiveApproval(
  sessionId: number,
  approvalId: number,
): Promise<LiveApprovalDecision> {
  const { data } = await api.post<{ code: number; data: LiveApprovalDecision }>(
    `/live/sessions/${sessionId}/approvals/${approvalId}/approve`,
  )
  return data.data
}

export async function rejectLiveApproval(
  sessionId: number,
  approvalId: number,
  reason: string,
): Promise<LiveApprovalDecision> {
  const { data } = await api.post<{ code: number; data: LiveApprovalDecision }>(
    `/live/sessions/${sessionId}/approvals/${approvalId}/reject`,
    { reason },
  )
  return data.data
}
