// pages/api/ec-by-country.ts
import type { NextApiRequest, NextApiResponse } from 'next'

interface EcByCountryData {
  country: string
  ec_contribution: number
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<EcByCountryData[] | { message: string }>
) {
  try {
    const upstream = await fetch(
      'http://54.93.51.85:8000/analytics/ec-by-country'
    )
    if (!upstream.ok) {
      return res.status(upstream.status).end()
    }
    const data: EcByCountryData[] = await upstream.json()
    // optional: cache on Vercel’s edge for 60s
    res.setHeader('Cache-Control', 's-maxage=60, stale-while-revalidate')
    return res.status(200).json(data)
  } catch (err) {
    console.error('EC proxy error:', err)
    return res.status(502).json({ message: 'Bad gateway' })
  }
}
