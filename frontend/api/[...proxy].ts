import type { VercelRequest, VercelResponse } from '@vercel/node'

export default async function handler(
  req: VercelRequest,
  res: VercelResponse
) {
  // Capture the path segments from req.query.proxy
  const raw = req.query.proxy as string | string[] | undefined
  const segments: string[] = Array.isArray(raw)
    ? raw
    : raw
    ? [raw]
    : []

  if (segments.length === 0) {
    return res.status(400).json({ message: 'No path to proxy provided' })
  }

  // Reconstruct path and preserve any query string
  const path = segments.join('/')
  const qsIndex = req.url?.indexOf('?')
  const qs = qsIndex !== undefined && qsIndex >= 0 ? req.url!.slice(qsIndex) : ''
  const upstreamUrl = `http://54.93.51.85:8000/${path}${qs}`

  try {
    const upstream = await fetch(upstreamUrl)
    if (!upstream.ok) {
      return res.status(upstream.status).end()
    }
    const data = await upstream.json()

    // Cache on Vercel edge for 60 seconds
    res.setHeader('Cache-Control', 's-maxage=60, stale-while-revalidate')
    return res.status(200).json(data)
  } catch (err) {
    console.error('Proxy error:', err)
    return res.status(502).json({ message: 'Bad gateway' })
  }
}
