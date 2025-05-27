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

interface CountryOption {
  code: string
  count: number
}

const ParticipationTrends: React.FC = () => {
  const [plotData, setPlotData] = useState<ProjectTimelineResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [country, setCountry] = useState('DE')
  const [availableCountries, setAvailableCountries] = useState<CountryOption[]>([])
  const [countriesLoading, setCountriesLoading] = useState(true)

  const fetchAvailableCountries = async () => {
    try {
      setCountriesLoading(true)
      const endpoint =
        process.env.NODE_ENV === 'development'
          ? '/analytics/available-countries' // CRA will proxy this to http://
          : '/api/analytics/available-countries' // Vercel will rewrite this to your catch-all
      const res = await axios.get<CountryOption[]>(endpoint)
      setAvailableCountries(res.data)
    } catch (err) {
      console.error('Failed to fetch available countries:', err)
    } finally {
      setCountriesLoading(false)
    }
  }

  const fetchData = async (selectedCountry: string) => {
    try {
      setLoading(true)
      const endpoint =
        process.env.NODE_ENV === 'development'
          ? `/analytics/participation-trends?country=${encodeURIComponent(selectedCountry)}` // CRA will proxy this to http://
          : `/api/analytics/participation-trends?country=${encodeURIComponent(selectedCountry)}` // Vercel will rewrite this to your catch-all
      const res = await axios.get<ProjectTimelineResponse>(endpoint)
      setPlotData(res.data)
    } catch (err) {
      console.error(err)
      setError('Failed to load participation trends data')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchAvailableCountries()
    fetchData(country)
  }, [])

  useEffect(() => {
    fetchData(country)
  }, [country])

  const handleCountryChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
    setCountry(event.target.value)
  }

  if (loading) return <p>Loading…</p>
  if (error) return <p style={{ color: 'red' }}>{error}</p>
  if (!plotData || plotData.data.length === 0) return <p>No participation data available for {country}.</p>

  const years = plotData.data.map(item => item.year)
  const values = plotData.data.map(item => item.metric_value)

  return (
    <div>
      <div style={{ marginBottom: '20px' }}>
        <label htmlFor="country-select" style={{ marginRight: '10px' }}>Country: </label>
        <select
          id="country-select"
          value={country}
          onChange={handleCountryChange}
          style={{ padding: '5px', minWidth: '200px' }}
          disabled={countriesLoading}
        >
          {countriesLoading ? (
            <option>Loading countries...</option>
          ) : (
            availableCountries.map(countryOption => (
              <option key={countryOption.code} value={countryOption.code}>
                {countryOption.code} ({countryOption.count} organizations)
              </option>
            ))
          )}
        </select>
      </div>
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
          title: { text: plotData.chart_title, font: {size: 14} },
          xaxis: {
            title: { text: plotData.x_axis_label, font: {size: 12} },
            automargin: true,
            tickfont: {size: 10}
          },
          yaxis: {
            title: { text: plotData.y_axis_label, font: {size: 12} },
            automargin: true,
            tickfont: {size: 10}
          },
          margin: { l: 60, r: 20, b: 60, t: 40, pad: 4 },
          showlegend: false,
          font: { size: 11 }
        }}
        style={{ width: '100%', height: '100%' }}
        config={{ responsive: true, displayModeBar: false }}
        useResizeHandler={true}
      />
    </div>
  )
}

export default ParticipationTrends
