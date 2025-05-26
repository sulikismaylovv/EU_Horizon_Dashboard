import React, { useState, useEffect } from 'react'
import axios from 'axios'
import Plot from 'react-plotly.js'

interface BubbleChartData {
  x: number
  y: number
  size: number
  label: string
  category?: string
}

interface BubbleChartResponse {
  chart_title: string
  data: BubbleChartData[]
  x_axis_label: string
  y_axis_label: string
  size_label: string
}

const EfficiencyBubbleChart: React.FC = () => {
  const [plotData, setPlotData] = useState<BubbleChartResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    (async () => {
      try {
        const res = await axios.get<BubbleChartResponse>('http://127.0.0.1:8000/analytics/efficiency-bubble-chart')
        setPlotData(res.data)
      } catch (err) {
        console.error(err)
        setError('Failed to load efficiency bubble chart data')
      } finally {
        setLoading(false)
      }
    })()
  }, [])

  if (loading) return <p>Loading…</p>
  if (error) return <p style={{ color: 'red' }}>{error}</p>
  if (!plotData || plotData.data.length === 0) return <p>No efficiency data available.</p>

  // Group data by category for different colors
  const categories = plotData.data.reduce<string[]>((acc, item) => {
    const category = item.category || 'Unknown'
    if (!acc.includes(category)) acc.push(category)
    return acc
  }, [])
  const colors = ['rgb(55, 128, 191)', 'rgb(255, 153, 51)', 'rgb(50, 171, 96)', 'rgb(255, 65, 54)', 'rgb(148, 52, 110)']

  const traces = categories.map((category, index) => {
    const categoryData = plotData.data.filter(item => (item.category || 'Unknown') === category)
    
    return {
      type: 'scatter' as const,
      mode: 'markers' as const,
      name: category,
      x: categoryData.map(item => item.x),
      y: categoryData.map(item => item.y),
      text: categoryData.map(item => item.label),
      marker: {
        size: categoryData.map(item => Math.sqrt(item.size) / 1000), // Scale down for visibility
        color: colors[index % colors.length],
        opacity: 0.7,
        line: { width: 1, color: 'white' }
      },
      hovertemplate: '<b>%{text}</b><br>' +
                     plotData.x_axis_label + ': €%{x:,.0f}<br>' +
                     plotData.y_axis_label + ': %{y} months<br>' +
                     plotData.size_label + ': €%{marker.size}<br>' +
                     'Program: %{fullData.name}<extra></extra>',
    }
  })

  return (
    <Plot
      data={traces}
      layout={{
        title: { text: plotData.chart_title },
        xaxis: {
          title: { text: plotData.x_axis_label },
          automargin: true,
          type: 'log', // Log scale for cost data
        },
        yaxis: {
          title: { text: plotData.y_axis_label },
          automargin: true,
        },
        hovermode: 'closest',
        showlegend: true,
        legend: {
          x: 1,
          y: 1,
          xanchor: 'left',
          yanchor: 'top'
        }
      }}
      style={{ width: '100%', height: '600px' }}
      config={{ responsive: true }}
    />
  )
}

export default EfficiencyBubbleChart
