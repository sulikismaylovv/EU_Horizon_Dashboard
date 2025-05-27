import React, { useState, useEffect } from 'react'
import axios from 'axios'
import Plot from 'react-plotly.js'
import { SunburstAPIData } from '../interfaces/Sunburst'

const SunburstDataViewer: React.FC = () => {
  const [data, setData] = useState<SunburstAPIData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    (async () => {
      try {
        setLoading(true)
        const endpoint =
          process.env.NODE_ENV === 'development'
            ? '/projects/analytics/sunburst' // CRA will proxy this to http://
            : '/api/projects/analytics/sunburst' // Vercel will rewrite this to your catch-all
        const res = await axios.get<SunburstAPIData>(endpoint)
        setData(res.data)
      } catch (err) {
        console.error(err)
        setError('Failed to load sunburst data')
      } finally {
        setLoading(false)
      }
    })()
  }, [])

  if (loading) return <p>Loading…</p>
  if (error) return <p style={{ color: 'red' }}>{error}</p>
  if (!data)   return <p>No data</p>

  return (
    <div style={{ width: '100%', height: '100%' }}>
      <Plot
        data={[
          {
            type: 'sunburst',
            labels:   data.labels,
            parents:  data.parents,
            values:   data.values,
            branchvalues: 'total',
            maxdepth: data.max_level_processed
          }
        ]}
        layout={{ 
          margin: { t: 20, l: 20, r: 20, b: 20 },
          font: { size: 11 },
          showlegend: false
        }}
        style={{ width: '100%', height: '100%' }}
        config={{ responsive: true, displayModeBar: false }}
        useResizeHandler={true}
      />
    </div>
  )
}

export default SunburstDataViewer
