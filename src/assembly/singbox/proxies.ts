// sing-box API(gRPC daemon.StartedService)后端的代理组装。
// 与 clash 的「拉取式」不同,这里是「流驱动」:订阅 SubscribeGroups / SubscribeOutbounds,
// 每次推送直接重建 assembly/proxies/state 的共享状态,因此选择/测速后无需手动刷新,
// 结果会随流自动回填到 UI。
import { getSingboxClient } from '@/api/singbox/client'
import type { StreamHandle } from '@/api/singbox/streams'
import { subscribeStream } from '@/api/singbox/subscriptions'
import type { Group, GroupItem, Groups, OutboundList } from '@/gen/daemon/started_service_pb'
import { iconReflectList, speedtestTimeout } from '@/store/settings'
import { activeBackend } from '@/store/setup'
import type { Proxy } from '@/types'
import type { ProxiesDriver, ProxiesPayload } from '../driver/types'
import { isLatencyTestable, proxyGroupList, proxyMap, proxyProviederList } from '../proxies/state'

const getHistoryFromItem = (item: GroupItem): Proxy['history'] =>
  item.urlTestDelay > 0
    ? [
        {
          time: new Date(Number(item.urlTestTime) * 1000).toISOString(),
          delay: item.urlTestDelay,
        },
      ]
    : []

const nodeToProxy = (item: GroupItem): Proxy => {
  return {
    name: item.tag,
    type: item.type,
    now: '',
    history: getHistoryFromItem(item),
    extra: {},
    icon: '',
  }
}

let groups = new Map<string, Group>()
let outbounds = new Map<string, GroupItem>()
let handles: StreamHandle[] = []
let sessionKey = ''
let ready: Promise<void> | null = null

// 一次 URLTest 的「结果指纹」:sing-box 把测速历史(时间戳 + 延迟)随
// SubscribeGroups / SubscribeOutbounds 推送，只保留最新一条且时间戳为秒级。
// 测速前记下指纹，只有指纹变了才说明本次结果真的到了。
//
// 注意:测速失败时内核会「删除」历史。若该节点此前从未测出过结果，删除前后快照
// 都是空，指纹不变 —— 这种情况从流里无法察觉，只能等测试预算耗尽后按失败结算。
type URLTestStamp = {
  time: bigint
  delay: number
}

const stampKey = (stamp?: URLTestStamp) => (stamp ? `${stamp.time}:${stamp.delay}` : '')

type URLTestWaiter = {
  // 本次测速要等到的目标(单节点是自身 tag，组是展开后的全部叶子成员)。
  targets: string[]
  // 登记等待时各目标的结果指纹。
  baseline: Map<string, string>
  resolve: () => void
  reject: (reason: Error) => void
  timer: ReturnType<typeof setTimeout>
}

const urlTestWaiters = new Set<URLTestWaiter>()

// 把两个流的数据合成 tag → 结果指纹。同一 tag 可能同时出现在出站流和组快照里，
// 两路到达有先后：组内成员一律以组快照为准，只有组里没有的 tag 才看出站流 ——
// 免得较旧的一份把「历史已被删除(延迟 0)」又盖回旧的延迟，或让两路互相误判。
const collectStamps = () => {
  const stamps = new Map<string, URLTestStamp>()

  for (const item of outbounds.values()) {
    stamps.set(item.tag, { time: item.urlTestTime, delay: item.urlTestDelay })
  }
  for (const group of groups.values()) {
    for (const item of group.items) {
      stamps.set(item.tag, { time: item.urlTestTime, delay: item.urlTestDelay })
    }
  }

  return stamps
}

const removeURLTestWaiter = (waiter: URLTestWaiter) => {
  urlTestWaiters.delete(waiter)
  clearTimeout(waiter.timer)
}

const resolveURLTestWaiter = (waiter: URLTestWaiter) => {
  if (!urlTestWaiters.delete(waiter)) return

  clearTimeout(waiter.timer)
  waiter.resolve()
}

// 流推送后结算等待:必须「本次测速的全部目标都已变化」才算整组测完。
// 不能用「某个目标一变就静置收尾」—— 内核按并发 10 分批测成员，提前结算会把
// 后面批次的结果漏掉，转圈提前结束、汇总数量也就跟着不对。
const settleURLTestWaiters = () => {
  if (!urlTestWaiters.size) return

  const stamps = collectStamps()

  for (const waiter of [...urlTestWaiters]) {
    const settled = waiter.targets.every(
      (tag) => stampKey(stamps.get(tag)) !== waiter.baseline.get(tag),
    )

    if (settled) resolveURLTestWaiter(waiter)
  }
}

const rejectURLTestWaiters = (reason: Error) => {
  for (const waiter of urlTestWaiters) {
    clearTimeout(waiter.timer)
    waiter.reject(reason)
  }
  urlTestWaiters.clear()
}

// 面板侧的测试预算:sing-box 的 URLTest RPC 不接受超时参数(内核自己用 TCPTimeout，
// 15s)，所以这里用面板的「测速超时」设置加 1s 余量作为等待上限，并保留 5s 下限，
// 与 clash 路径 Math.max(5000, speedtestTimeout) 的语义一致。预算耗尽仍未等到指纹
// 变化时，按「本次测试已结束但没拿到新结果」结算(失败节点即 NOT_CONNECTED)，而不是
// 抛错 —— 节点自己连不上不是面板的请求错误。clash 驱动同样是把失败节点作为 delay=0
// 返回，共享的 assembly/proxies/latency 也按此假设处理。若内核在预算之后才推来结果，
// 订阅仍会照常回填界面。
const URL_TEST_BUDGET_FLOOR = 5000
const URL_TEST_BUDGET_MARGIN = 1000

const urlTestBudget = (timeout: number) =>
  Math.max(URL_TEST_BUDGET_FLOOR, timeout) + URL_TEST_BUDGET_MARGIN

const waitForURLTestResult = (targets: string[], baseline: Map<string, string>, budget: number) => {
  let waiter!: URLTestWaiter
  const promise = new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      if (!urlTestWaiters.has(waiter)) return
      removeURLTestWaiter(waiter)
      resolve()
    }, budget)

    waiter = { targets, baseline, resolve, reject, timer }
    urlTestWaiters.add(waiter)
  })

  return {
    promise,
    cancel: () => removeURLTestWaiter(waiter),
  }
}

// 由流数据原生组装共享状态(无 clash 的 provider / GLOBAL / 排序等概念)。
const buildPayload = (): ProxiesPayload => {
  const proxies: Record<string, Proxy> = {}

  // 1) 出站叶子节点(含延迟)
  for (const item of outbounds.values()) {
    proxies[item.tag] = nodeToProxy(item)
  }
  // 2) 用组内 items 补建缺失的叶子节点(outbounds 流可能晚到或不含某些成员)
  for (const group of groups.values()) {
    for (const item of group.items) {
      if (!proxies[item.tag]) proxies[item.tag] = nodeToProxy(item)
    }
  }
  // 3) 分组条目(携带 all / now),始终覆盖同名节点
  for (const group of groups.values()) {
    proxies[group.tag] = {
      name: group.tag,
      type: group.type,
      now: group.selected,
      all: group.items.map((i) => i.tag),
      selectable: group.selectable,
      history: [],
      extra: {},
      icon: '',
    }
  }
  // 4) 把组内 items 的延迟回填到叶子节点(绝不动带 all 的组条目)
  for (const group of groups.values()) {
    for (const item of group.items) {
      const node = proxies[item.tag]
      if (node && !node.all?.length && item.urlTestDelay > 0) {
        node.history = getHistoryFromItem(item)
      }
    }
  }
  // 5) 应用用户配置的「名称→图标」映射(与 clash 一致,sing-box 流不含图标)
  for (const iconReflect of iconReflectList.value) {
    const node = proxies[iconReflect.name]
    if (node) node.icon = iconReflect.icon
  }

  return {
    proxies,
    providers: [],
  }
}

const applyPayload = () => {
  const payload = buildPayload()

  proxyMap.value = payload.proxies
  proxyGroupList.value = Array.from(groups.values())
    .filter((g) => g.items.length)
    .map((g) => g.tag)
  proxyProviederList.value = payload.providers

  return payload
}

const closeStreams = () => {
  handles.forEach((h) => h.close())
  handles = []
  rejectURLTestWaiters(new Error('sing-box proxy stream closed'))
  sessionKey = ''
  ready = null
}

const stop = () => {
  closeStreams()
  groups = new Map()
  outbounds = new Map()
}

const ensureSession = () => {
  const backend = activeBackend.value
  const client = getSingboxClient()?.client
  if (!backend || backend.type !== 'singbox' || !client) {
    stop()
    return
  }
  if (sessionKey === backend.uuid && handles.length) return

  stop()
  sessionKey = backend.uuid

  let resolveReady!: () => void
  let resolved = false
  ready = new Promise<void>((r) => (resolveReady = r))

  // 切走后上游只会调用「新后端」驱动的 reset(见 assembly/session),所以订阅可能
  // 带着旧会话继续推送。这里在每次推送时自检:已不是本会话就地停掉,免得旧后端的
  // 代理数据覆盖新后端的列表。
  const alive = () =>
    activeBackend.value?.type === 'singbox' && activeBackend.value.uuid === sessionKey

  handles = [
    subscribeStream<Groups>('groups', (msg) => {
      if (!alive()) {
        stop()
        return
      }
      groups = new Map()
      for (const g of msg.group) groups.set(g.tag, g)
      applyPayload()
      if (!resolved) {
        resolved = true
        resolveReady()
      }
      // URLTest RPC 只负责启动任务；历史记录更新后，结果才会通过订阅推送。
      // 这里按各次测速的目标指纹结算，别让无关推送把等待提前唤醒。
      settleURLTestWaiters()
    }),
    subscribeStream<OutboundList>('outbounds', (msg) => {
      if (!alive()) {
        stop()
        return
      }
      outbounds = new Map()
      for (const o of msg.outbounds) outbounds.set(o.tag, o)
      applyPayload()
      settleURLTestWaiters()
    }),
  ]
}

// 在后端切换 / 登出时丢弃订阅。
export const resetProxies = () => stop()

// 内核的组测速会递归测试成员(含嵌套组里的叶子)，按并发 10 分批。这里把等待目标
// 展开成「真正会产出历史的叶子」:嵌套组展开、reject/block 这类永远测不出结果的
// 成员剔除 —— 否则等待器会空等它们直到预算耗尽，或者反过来提前结算。
const collectURLTestTargets = (outboundTag: string) => {
  const group = groups.get(outboundTag)
  if (!group) return [outboundTag]

  const targets: string[] = []
  const seen = new Set<string>()
  const visit = (tag: string) => {
    if (seen.has(tag)) return
    seen.add(tag)

    const node = proxyMap.value[tag]
    if (node?.all?.length) {
      node.all.forEach(visit)
      return
    }
    if (isLatencyTestable(tag)) targets.push(tag)
  }
  group.items.forEach((item) => visit(item.tag))

  return targets.length ? targets : [outboundTag]
}

const runURLTest = async (outboundTag: string, timeout = speedtestTimeout.value) => {
  ensureSession()
  if (ready) await ready

  const client = getSingboxClient()?.client
  if (!client) return

  // 先记下测速前的结果指纹，之后只有指纹变化才说明本次结果到了 —— 避免别处推送
  // 触发误判。组测速要等展开后的全部叶子，单测只等自身 tag。
  const targets = collectURLTestTargets(outboundTag)
  const stamps = collectStamps()
  const baseline = new Map(targets.map((tag) => [tag, stampKey(stamps.get(tag))]))
  const budget = urlTestBudget(timeout)

  // 先注册等待，避免测速很快时结果推送早于一元 RPC 响应而丢失。
  const result = waitForURLTestResult(targets, baseline, budget)
  // 一元 RPC 只负责「启动任务」，结果靠订阅推送回收。给它一个硬上限:即使调用卡住，
  // 也不能把测速中的转圈状态永远吊住。
  const controller = new AbortController()
  const abortTimer = setTimeout(() => controller.abort(), budget + URL_TEST_BUDGET_MARGIN)
  try {
    await Promise.all([
      client.uRLTest({ outboundTag }, { signal: controller.signal }).catch((e) => {
        // 预算耗尽后是我们主动中止的:内核侧测试照常跑完、结果仍会随流回填，
        // 不算请求失败，交给等待器结算。
        if (controller.signal.aborted) return
        throw e
      }),
      result.promise,
    ])
  } finally {
    clearTimeout(abortTimer)
    result.cancel()
  }
}

export const proxiesDriver: ProxiesDriver = {
  fetch: async () => {
    ensureSession()
    if (ready) await ready

    return applyPayload()
  },

  select: async (group, name) => {
    const client = getSingboxClient()?.client
    const proxyGroup = proxyMap.value[group]
    if (!client || proxyGroup?.selectable === false) return

    await client.selectOutbound({ groupTag: group, outboundTag: name })

    // 乐观更新,流随后会确认
    const target = groups.get(group)
    if (target) {
      target.selected = name
      applyPayload()
    }
  },

  clearFixed: async () => {},

  // sing-box API 支持直接测试单个 outbound;节点卡片传节点自身的 tag。
  testNode: async (name, _url, timeout) => {
    await runURLTest(name, timeout)

    return proxyMap.value[name]?.history?.at(-1)?.delay ?? 0
  },
  testProviderNode: async (_provider, name, _url, timeout) => {
    await runURLTest(name, timeout)

    return proxyMap.value[name]?.history?.at(-1)?.delay ?? 0
  },
  testGroup: async (group, _url, timeout) => {
    await runURLTest(group, timeout)

    const result: Record<string, number> = {}
    for (const item of groups.get(group)?.items ?? []) {
      result[item.tag] = item.urlTestDelay
    }

    return result
  },

  updateProvider: async () => {},
  healthCheckProvider: async () => {},
  fetchSmartWeights: async () => ({}),
  flushSmartWeights: async () => {},
}
