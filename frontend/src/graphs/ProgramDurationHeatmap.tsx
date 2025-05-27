import React, { useState, useEffect } from 'react'
import axios from 'axios'
import Plot from 'react-plotly.js'

interface HeatmapData {
  x_category: string
  y_category: string
  value: number
}

interface HeatmapResponse {
  chart_title: string
  data: HeatmapData[]
  x_axis_label: string
  y_axis_label: string
  color_scale_label: string
}

const ProgramDurationHeatmap: React.FC = () => {
  const [plotData, setPlotData] = useState<HeatmapResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    (async () => {
      try {
        const endpoint =
          process.env.NODE_ENV === 'development'
            ? '/analytics/program-duration-heatmap' // CRA will proxy this to http://
            : '/api/analytics/program-duration-heatmap' // Vercel will rewrite this to your catch-all
        const res = await axios.get<HeatmapResponse>(endpoint)
        setPlotData(res.data)
      } catch (err) {
        console.error(err)
        setError('Failed to load program duration heatmap data')
      } finally {
        setLoading(false)
      }
    })()
  }, [])

  if (loading) return <p>Loading…</p>
  if (error) return <p style={{ color: 'red' }}>{error}</p>
  if (!plotData || plotData.data.length === 0) return <p>No heatmap data available.</p>

  // Get unique categories for axes
  const xCategories = plotData.data.reduce<string[]>((acc, item) => {
    if (!acc.includes(item.x_category)) acc.push(item.x_category)
    return acc
  }, []).sort()
  
  const yCategories = plotData.data.reduce<string[]>((acc, item) => {
    if (!acc.includes(item.y_category)) acc.push(item.y_category)
    return acc
  }, []).sort()

  // Create 2D array for heatmap values
  const zValues = yCategories.map(yCategory => 
    xCategories.map(xCategory => {
      const dataPoint = plotData.data.find(
        item => item.x_category === xCategory && item.y_category === yCategory
      )
      return dataPoint ? dataPoint.value : null
    })
  )

  // Create hover text
  const hoverText = yCategories.map(yCategory => 
    xCategories.map(xCategory => {
      const dataPoint = plotData.data.find(
        item => item.x_category === xCategory && item.y_category === yCategory
      )
      return dataPoint 
        ? `${yCategory} × ${xCategory}<br>Duration: ${dataPoint.value.toFixed(1)} months`
        : ''
    })
  )

  return (
    <Plot
      data={[
        {
          type: 'heatmap',
          z: zValues,
          x: xCategories,
          y: yCategories,
          hovertemplate: '%{text}<extra></extra>',
          text: hoverText as any, // Type assertion to fix Plotly type issue
          colorscale: 'Viridis',
          colorbar: {
            title: { text: plotData.color_scale_label }, // Fix colorbar title type
          },
        },
      ]}
      layout={{
        title: { text: plotData.chart_title, font: {size: 14} },
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
        margin: { l: 80, r: 60, b: 80, t: 40, pad: 4 },
        font: { size: 11 }
      }}
      style={{ width: '100%', height: '100%' }}
      config={{ responsive: true, displayModeBar: false }}
      useResizeHandler={true}
    />
  )
}

export default ProgramDurationHeatmap
