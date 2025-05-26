import React, { useState, useEffect } from 'react'
import axios from 'axios'
import Plot from 'react-plotly.js'

interface DistributionData {
  label: string
  value: number
  count: number
}

interface DistributionResponse {
  chart_title: string
  data: DistributionData[]
  chart_type: string
  x_axis_label: string
  y_axis_label: string
}

const FundingDistribution: React.FC = () => {
  const [plotData, setPlotData] = useState<DistributionResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [binCount, setBinCount] = useState(20)

  const fetchData = async (bins: number) => {
    try {
      setLoading(true)
      const res = await axios.get<DistributionResponse>(
        `http://127.0.0.1:8000/analytics/funding-distribution?bin_count=${bins}`
      )
      setPlotData(res.data)
    } catch (err) {
      console.error(err)
      setError('Failed to load funding distribution data')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData(binCount)
  }, [binCount])

  if (loading) return <p>Loading…</p>
  if (error) return <p style={{ color: 'red' }}>{error}</p>
  if (!plotData || plotData.data.length === 0) return <p>No funding distribution data available.</p>

  const labels = plotData.data.map(item => item.label)
  const counts = plotData.data.map(item => item.count)

  return (
    <div>
      <div style={{ marginBottom: '20px' }}>
        <label htmlFor="bin-count">Number of Bins: </label>
        <input
          id="bin-count"
          type="number"
          value={binCount}
          onChange={(e) => setBinCount(parseInt(e.target.value) || 20)}
          min="5"
          max="50"
          style={{ width: '60px', marginLeft: '10px' }}
        />
      </div>
      <Plot
        data={[
          {
            type: 'bar',
            x: labels,
            y: counts,
            marker: {
              color: 'rgb(158, 202, 225)',
              line: { color: 'rgb(8, 48, 107)', width: 1 }
            },
          },
        ]}
        layout={{
          title: { text: plotData.chart_title },
          xaxis: {
            title: { text: plotData.x_axis_label },
            automargin: true,
            tickangle: -45,
          },
          yaxis: {
            title: { text: plotData.y_axis_label },
            automargin: true,
          },
        }}
        style={{ width: '100%', height: '500px' }}
        config={{ responsive: true }}
      />
    </div>
  )
}

export default FundingDistribution
