import React, { useState, useEffect } from 'react'
import axios from 'axios'
import Plot from 'react-plotly.js'

interface InstitutionData {
  name: string
  ec_contribution: number
}

const TopInstitutionsBarChart: React.FC = () => {
  const [plotData, setPlotData] = useState<InstitutionData[] | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    (async () => {
      try {
        const res = await axios.get<InstitutionData[]>('http://54.93.51.85:8000/analytics/top-institutions-by-funding')
        // Remove the first element from the response data
        if (res.data && res.data.length > 0) {
          setPlotData(res.data.slice(1)) // Slice the array starting from the second element
        } else {
          setPlotData([]) // Set to empty array if response is empty or null
        }
      } catch (err) {
        console.error(err)
        setError('Failed to load top institutions data')
      } finally {
        setLoading(false)
      }
    })()
  }, [])

  if (loading) return <p>Loading…</p>
  if (error) return <p style={{ color: 'red' }}>{error}</p>
  // Updated check to handle potentially empty plotData after slicing
  if (!plotData || plotData.length === 0) return <p>No data available to display (after filtering).</p>

  // Prepare data for Plotly
  const institutionNames = plotData.map(item => item.name)
  const ecContributions = plotData.map(item => item.ec_contribution)

  return (
    <Plot
      data={[
        {
          type: 'bar',
          x: institutionNames,
          y: ecContributions,
          marker: {
            color: 'rgb(26, 118, 255)',
          },
        },
      ]}
      layout={{
        title: {text:'Top Institutions by EC Contribution', font: {size: 14}}, 
        xaxis: {
          title:{text: 'Institution', font: {size: 12}},
          automargin: true,
          tickangle: -45,
          tickfont: {size: 10}
        },
        yaxis: {
          title:{text: 'EC Contribution (EUR)', font: {size: 12}},
          automargin: true,
          tickfont: {size: 10}
        },
        margin: { l: 80, r: 20, b: 120, t: 40, pad: 4 },
        showlegend: false,
        font: { size: 11 }
      }}
      style={{ width: '100%', height: '100%' }}
      config={{ responsive: true, displayModeBar: false }}
      useResizeHandler={true}
    />
  )
}

export default TopInstitutionsBarChart