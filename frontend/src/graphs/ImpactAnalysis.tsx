import React, { useState, useEffect } from 'react'
import axios from 'axios'
import Plot from 'react-plotly.js'

// Define the interface for the project data
interface ProjectImpactData {
  id: number
  acronym: string
  status: string
  title: string
  total_cost: number | string | null
  ec_max_contribution: number | null
  duration_days: number | null
  duration_months: number | null
  duration_years: number | null
  n_institutions: number | null
  ec_contribution_per_year: number | null
  total_cost_per_year: number | null
  niche: string
  n_publications: number | null
  duration_months_remainder: number | null
}

const metricsList = [
  'total_cost',
  'ec_max_contribution',
  'total_cost_per_year',
  'ec_contribution_per_year',
  'n_institutions',
  'n_publications',
  'duration_days'
] as const

const metricsLabels: Record<string, string> = {
  total_cost: 'Total cost [€]',
  ec_max_contribution: 'EU funding [€]',
  total_cost_per_year: 'Total cost per year [€]',
  ec_contribution_per_year: 'EU funding per year [€]',
  n_institutions: 'Number of collaborating organizations',
  n_publications: 'Number of publications',
  duration_days: 'Project duration [days]'
}

const hoverDataList = [
  'niche',
  'ec_max_contribution',
  'total_cost',
  'n_institutions',
  'n_publications',
  'duration_years',
  'duration_months_remainder'
] as const

const ImpactAnalysis: React.FC = () => {
  const [data, setData] = useState<ProjectImpactData[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [xMetric, setXMetric] = useState<typeof metricsList[number]>('total_cost')
  const [yMetric, setYMetric] = useState<typeof metricsList[number]>('ec_max_contribution')

  useEffect(() => {
    (async () => {
      try {
        const res = await axios.get<ProjectImpactData[]>('http://127.0.0.1:8000/projects_impact_analysis')
        setData(res.data)
      } catch (err) {
        setError('Failed to load impact analysis data')
      } finally {
        setLoading(false)
      }
    })()
  }, [])

  if (loading) return <p>Loading…</p>
  if (error) return <p style={{ color: 'red' }}>{error}</p>
  if (!data || data.length === 0) return <p>No impact analysis data available.</p>

  // Prepare data for Plotly
  const getMetricArray = (metric: string) =>
    data.map(d => {
      const val = (d as any)[metric]
      if (typeof val === 'string') {
        // Try to parse as float, fallback to null if not possible
        const parsed = parseFloat(val)
        return isNaN(parsed) ? null : parsed
      }
      return val !== undefined ? val : null
    })

  const customData = data.map(d =>
    hoverDataList.map(k => (d as any)[k])
  )

  const hoverTemplate =
    '<b>%{hovertext}</b>' +
    '<br>Research fields: %{customdata[0]}' +
    '<br>EU funding contribution: €%{customdata[1]}' +
    '<br>Total Cost:  €%{customdata[2]}' +
    '<br># collaborating organizations: %{customdata[3]}' +
    '<br># publications: %{customdata[4]}' +
    '<br>Duration: %{customdata[5]} years %{customdata[6]} months' +
    '<extra></extra>'

  // Dropdowns for axis selection
  const axisSelector = (
    axis: 'x' | 'y',
    value: typeof metricsList[number],
    setValue: React.Dispatch<React.SetStateAction<typeof metricsList[number]>>
  ) => (
    <label style={{ marginRight: 16 }}>
      {axis.toUpperCase()}-axis:&nbsp;
      <select value={value} onChange={e => setValue(e.target.value as typeof metricsList[number])}>
        {metricsList.map(m => (
          <option key={m} value={m}>
            {metricsLabels[m]}
          </option>
        ))}
      </select>
    </label>
  )

  return (
    <div>
      <h2>Impact Analysis</h2>
      <div style={{ marginBottom: 16 }}>
        {axisSelector('x', xMetric, setXMetric)}
        {axisSelector('y', yMetric, setYMetric)}
      </div>
      <Plot
        data={[
          {
            x: getMetricArray(xMetric),
            y: getMetricArray(yMetric),
            text: data.map(d => d.title),
            customdata: customData,
            type: 'scattergl',
            mode: 'markers',
            marker: { size: 7, opacity: 0.7, color: '#636efa' },
            hovertemplate: hoverTemplate,
            hovertext: data.map(d => d.title)
          }
        ]}
        layout={{
          title: { text: 'Impact analysis' },
          xaxis: { title: {text: metricsLabels[xMetric] }},
          yaxis: { title: {text: metricsLabels[yMetric] }},
          autosize: true,
          margin: { l: 60, r: 40, b: 60, t: 60 },
        }}
        style={{ width: '100%', height: '600px' }}
        config={{ responsive: true }}
      />
    </div>
  )
}

export default ImpactAnalysis

