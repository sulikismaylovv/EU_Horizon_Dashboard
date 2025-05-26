import React, { useState, useEffect } from 'react'
import axios from 'axios'
import Plot from 'react-plotly.js'

interface TimeSeriesData {
  year: number
  metric_value: number
  metric_name: string
}

interface ProjectTimelineResponse {
  chart_title: string
  x_axis_label: string
  y_axis_label: string
  data: TimeSeriesData[]
  chart_type: string
}

const ParticipationTrends: React.FC = () => {
  const [plotData, setPlotData] = useState<ProjectTimelineResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [country, setCountry] = useState('Germany')

  const fetchData = async (selectedCountry: string) => {
    try {
      setLoading(true)
      const res = await axios.get<ProjectTimelineResponse>(
        `http://127.0.0.1:8000/analytics/participation-trends?country=${encodeURIComponent(selectedCountry)}`
      )
      setPlotData(res.data)
    } catch (err) {
      console.error(err)
      setError('Failed to load participation trends data')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData(country)
  }, [country])

  const handleCountryChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setCountry(event.target.value)
  }

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault()
    fetchData(country)
  }

  if (loading) return <p>Loading…</p>
  if (error) return <p style={{ color: 'red' }}>{error}</p>
  if (!plotData || plotData.data.length === 0) return <p>No participation data available for {country}.</p>

  const years = plotData.data.map(item => item.year)
  const values = plotData.data.map(item => item.metric_value)

  return (
    <div>
      <form onSubmit={handleSubmit} style={{ marginBottom: '20px' }}>
        <label htmlFor="country-input">Country: </label>
        <input
          id="country-input"
          type="text"
          value={country}
          onChange={handleCountryChange}
          placeholder="Enter country name..."
          style={{ marginLeft: '10px', marginRight: '10px', padding: '5px' }}
        />
        <button type="submit">Update Chart</button>
      </form>
      <Plot
        data={[
          {
            type: 'scatter',
            mode: 'lines+markers',
            fill: 'tonexty',
            x: years,
            y: values,
            line: { color: 'rgb(75, 192, 192)', width: 2 },
            marker: { size: 6, color: 'rgb(75, 192, 192)' },
            fillcolor: 'rgba(75, 192, 192, 0.2)',
          },
        ]}
        layout={{
          title: { text: plotData.chart_title },
          xaxis: {
            title: { text: plotData.x_axis_label },
            automargin: true,
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

export default ParticipationTrends
