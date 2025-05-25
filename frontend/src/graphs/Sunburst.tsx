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
        const res = await axios.get<SunburstAPIData>('http://127.0.0.1:8000/projects/analytics/sunburst')
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
    <div>
      <h2>Sunburst: {data.metric_name}</h2>
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
        layout={{ margin: { t: 0, l: 0, r: 0, b: 0 } }}
        style={{ width: '100%', height: '600px' }}
      />
    </div>
  )
}

export default SunburstDataViewer
