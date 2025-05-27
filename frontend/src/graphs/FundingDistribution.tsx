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

  const fetchData = async () => {
    try {
      setLoading(true)
      const endpoint =
        process.env.NODE_ENV === 'development'
          ? `/analytics/funding-distribution` // CRA will proxy this to http://
          : `/api/analytics/funding-distribution` // Vercel will rewrite this to your catch-all
      const res = await axios.get<DistributionResponse>(endpoint)
      setPlotData(res.data)
    } catch (err) {
      console.error(err)
      setError('Failed to load funding distribution data')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData()
  }, [])

  if (loading) return <p>Loading…</p>
  if (error) return <p style={{ color: 'red' }}>{error}</p>
  if (!plotData || plotData.data.length === 0) return <p>No funding distribution data available.</p>

  const labels = plotData.data.map(item => item.label)
  const counts = plotData.data.map(item => item.count)

  // Sort data by funding ranges to ensure proper order
  const sortedData = [...plotData.data].sort((a, b) => a.value - b.value)
  const sortedLabels = sortedData.map(item => item.label)
  const sortedCounts = sortedData.map(item => item.count)

  return (
    <div>
      <div style={{ marginBottom: '20px' }}>
        <p style={{ fontSize: '14px', color: '#666' }}>
          Distribution shows project counts across meaningful funding ranges. 
          Total projects: {counts.reduce((sum, count) => sum + count, 0)}
        </p>
      </div>
      <Plot
        data={[
          {
            type: 'bar',
            x: sortedLabels,
            y: sortedCounts,
            marker: {
              color: sortedCounts.map((count, index) => {
                // Color gradient based on count
                const intensity = Math.min(count / Math.max(...sortedCounts), 1)
                return `rgba(26, 118, 255, ${0.3 + intensity * 0.7})`
              }),
              line: { color: 'rgb(8, 48, 107)', width: 1 }
            },
            text: sortedCounts.map(count => count.toLocaleString()),
            textposition: 'outside',
            hovertemplate: '<b>%{x}</b><br>' +
                          'Projects: %{y:,}<br>' +
                          '<extra></extra>'
          },
        ]}
        layout={{
          title: { text: plotData.chart_title, font: {size: 16, family: 'Arial, sans-serif'} },
          xaxis: {
            title: { text: plotData.x_axis_label, font: {size: 12} },
            automargin: true,
            tickangle: -45,
            tickfont: {size: 10}
          },
          yaxis: {
            title: { text: plotData.y_axis_label, font: {size: 12} },
            automargin: true,
            tickfont: {size: 10}
          },
          margin: { l: 70, r: 30, b: 100, t: 60, pad: 4 },
          showlegend: false,
          font: { size: 11 },
          plot_bgcolor: 'rgba(0,0,0,0)',
          paper_bgcolor: 'rgba(0,0,0,0)'
        }}
        style={{ width: '100%', height: '100%' }}
        config={{ responsive: true, displayModeBar: false }}
        useResizeHandler={true}
      />
    </div>
  )
}

export default FundingDistribution
