import React, { useState, useEffect } from 'react'
import axios from 'axios'
import Plot from 'react-plotly.js'

// Interface for the EC by Country data structure
interface EcByCountryData {
  country: string
  ec_contribution: number
}

const EcByCountryBarChart: React.FC = () => {
  // State to hold an array of EcByCountryData
  const [plotData, setPlotData] = useState<EcByCountryData[] | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    (async () => {
      try {
        // Expecting an array from the API
        const res = await axios.get<EcByCountryData[]>('/analytics/ec-by-country')
       // Updated API endpoint
        
        // Remove the first element from the response data
        if (res.data && res.data.length > 0) {
          setPlotData(res.data.slice(1)) // Slice the array starting from the second element
        } else {
          setPlotData([]) // Set to empty array if response is empty or null
        }
      } catch (err) {
        console.error(err)
        setError('Failed to load EC contribution by country data') // Updated error message
      } finally {
        setLoading(false)
      }
    })()
  }, [])

  if (loading) return <p>Loading…</p>
  if (error) return <p style={{ color: 'red' }}>{error}</p>
  // Updated check to handle potentially empty plotData after slicing
  if (!plotData || plotData.length === 0) return <p>No EC contribution data by country available to display (after filtering).</p>

  // Prepare data for Plotly
  const countryCodes = plotData.map(item => item.country)
  const ecContributions = plotData.map(item => item.ec_contribution)

  return (
    <Plot
      data={[
        {
          type: 'bar', // Specify bar chart
          x: countryCodes,
          y: ecContributions,
          marker: {
            color: 'rgb(255, 193, 7)', // Example color (a shade of amber/yellow)
          },
        },
      ]}
      layout={{
        title: {text:'EC Contribution by Country', font: {size: 14}}, 
        xaxis: {
          title: {text:'Country Code', font: {size: 12}}, 
          automargin: true,
          tickangle: -45,
          tickfont: {size: 10}
        },
        yaxis: {
          title: {text:'EC Contribution (EUR)', font: {size: 12}}, 
          automargin: true,
          tickfont: {size: 10}
        },
        margin: { l: 80, r: 20, b: 80, t: 40, pad: 4 },
        showlegend: false,
        font: { size: 11 }
      }}
      style={{ width: '100%', height: '100%' }}
      config={{ responsive: true, displayModeBar: false }}
      useResizeHandler={true}
    />
  )
}

export default EcByCountryBarChart