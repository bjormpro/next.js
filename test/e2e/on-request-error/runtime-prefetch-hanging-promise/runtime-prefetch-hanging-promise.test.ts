import { nextTestSetup } from 'e2e-utils'
import { retry } from 'next-test-utils'
import type * as Playwright from 'playwright'
import { createRouterAct } from 'router-act'

const HANGING_PROMISE_MESSAGE =
  'During prerendering, `connection()` rejects when the prerender is complete'

describe('on-request-error - runtime prefetch hanging promise', () => {
  const { next, isNextDev, skipped } = nextTestSetup({
    files: __dirname,
    skipDeployment: true,
  })
  if (skipped) {
    return
  }
  if (isNextDev) {
    // Runtime prefetching only happens in production builds.
    it('is skipped in dev', () => {})
    return
  }

  it('reports the render error caused by a module-level cache keyed on the headers object', async () => {
    const cliOutputStart = next.cliOutput.length

    let page: Playwright.Page
    const browser = await next.browser('/', {
      beforePageLoad(p: Playwright.Page) {
        page = p
      },
    })
    const act = createRouterAct(page)

    // Reveal the link to trigger a runtime prefetch of /dynamic. The prefetch
    // includes the static parts of the page but must omit the dynamic content
    // gated on connection().
    await act(async () => {
      const linkToggle = await browser.elementByCss(
        'input[data-link-accordion="/dynamic"]'
      )
      await linkToggle.click()
    }, [
      { includes: 'Header:' },
      { includes: 'Dynamic content', block: 'reject' },
    ])

    // The runtime prefetch consists of a prospective prerender (to fill
    // caches) and a final prerender. Aborting the prospective prerender
    // rejects its hanging connection() promise. Because the page memoizes the
    // promise in a module-level cache keyed on the identity of the headers
    // object, which both prerenders share, the final prerender awaits the
    // same, now-rejected promise and genuinely fails to render the dynamic
    // subtree. This is a real error caused by the unsound caching pattern
    // (request-scoped memoization must not outlive a render pass; use
    // React.cache instead), so it must be reported to instrumentation
    // onRequestError and logged to the server console.
    await retry(async () => {
      const cliOutput = next.cliOutput.slice(cliOutputStart)
      expect(cliOutput).toContain(
        `[instrumentation] onRequestError:${HANGING_PROMISE_MESSAGE}`
      )
      expect(cliOutput).toContain(`⨯ Error: ${HANGING_PROMISE_MESSAGE}`)
    }, 5000)
  })

  it('breaks the dynamic content of a navigation when the spawned runtime prerender created the memoized promise', async () => {
    const cliOutputStart = next.cliOutput.length

    let page: Playwright.Page
    const browser = await next.browser('/', {
      beforePageLoad(p: Playwright.Page) {
        page = p
      },
    })
    const act = createRouterAct(page)

    // Reveal the link, triggering the runtime prefetch.
    await act(async () => {
      const linkToggle = await browser.elementByCss(
        'input[data-link-accordion="/dynamic"]'
      )
      await linkToggle.click()
    }, { includes: 'Header:' })

    // Navigate. connection() resolves during navigations, so the page should
    // render its dynamic content. But the navigation request also spawns a
    // runtime prerender to refresh the client's prefetch cache, which shares
    // the request's headers object. The prerender reaches the module-level
    // cache first and memoizes a hanging connection() promise, which rejects
    // when the prerender is aborted. The navigation's dynamic render awaits
    // the same promise and fails: the user gets the error boundary instead of
    // the dynamic content.
    await browser.elementByCss('a[href="/dynamic"]').click()

    await retry(async () => {
      expect(await browser.elementById('dynamic-error').text()).toBe(
        'Failed to render dynamic content'
      )
    })
    expect(await browser.hasElementByCssSelector('#dynamic-content')).toBe(
      false
    )

    // The navigation request reports the render error as well.
    await retry(async () => {
      const cliOutput = next.cliOutput.slice(cliOutputStart)
      expect(cliOutput).toContain(
        `[instrumentation] onRequestError:${HANGING_PROMISE_MESSAGE}`
      )
    }, 5000)
  })
})
