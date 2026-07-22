import { Suspense } from 'react'
import { headers } from 'next/headers'
import { connection } from 'next/server'
import { setTimeout } from 'timers/promises'

export const instant = {
  unstable_samples: [{ headers: [['host', 'test-host']] }],
}
export const prefetch = 'allow-runtime'

export default function Page() {
  return (
    <main>
      <p id="intro">This page gates its dynamic content on connection().</p>
      <Suspense fallback={<div style={{ color: 'grey' }}>Loading 1...</div>}>
        <RuntimePrefetchable />
      </Suspense>
    </main>
  )
}

async function RuntimePrefetchable() {
  const headersStore = await headers()
  const headerValue = headersStore.get('host') === null ? 'missing' : 'present'
  return (
    <div>
      <div id="header-value">{`Header: ${headerValue}`}</div>
      <Suspense fallback={<div style={{ color: 'grey' }}>Loading 2...</div>}>
        <Dynamic />
      </Suspense>
    </div>
  )
}

// UNSOUND PATTERN, used here to document its failure mode: a module-level
// cache keyed on the identity of the headers object (like `dedupe()` from the
// Flags SDK, or any per-request memoization that treats the headers object as
// "the request"), gating its data on `connection()` so it only produces data
// during actual navigations, never during (runtime) prefetches.
//
// The headers object is shared between all render passes of a request: the
// prospective and final prerenders of a runtime prefetch, or a navigation's
// dynamic render and the runtime prerender that is spawned from it to refresh
// the client's prefetch cache. Those passes have different semantics for
// `connection()`: in prerenders the promise hangs and is rejected when the
// pass is aborted, during navigations it resolves. A promise memoized in one
// pass therefore breaks the other pass that consumes it. Request-scoped
// memoization must not outlive a render pass; use React.cache instead, which
// is scoped to a single render.
const requestDataCache = new WeakMap<object, Promise<string>>()
async function getRequestData(): Promise<string> {
  const headersStore = await headers()
  let dataPromise = requestDataCache.get(headersStore)
  if (dataPromise === undefined) {
    dataPromise = (async () => {
      await connection()
      return 'request data'
    })()
    requestDataCache.set(headersStore, dataPromise)
  }
  return dataPromise
}

async function Dynamic() {
  const workUnitStore = (
    require('next/dist/server/app-render/work-unit-async-storage.external') as typeof import('next/dist/server/app-render/work-unit-async-storage.external')
  ).workUnitAsyncStorage.getStore()
  switch (workUnitStore?.type) {
    case 'request':
      // During a navigation, give the runtime prerender that was spawned from
      // this request a head start, so it reaches getRequestData() first and
      // the memoized promise is created in the prerender context, where
      // connection() hangs. In real apps this ordering is a race, e.g. when
      // the dynamic render suspends on slower IO than the prerender does.
      await setTimeout(50)
      break
    default:
      break
  }
  const data = await getRequestData()
  return <div id="dynamic-content">Dynamic content: {data}</div>
}
