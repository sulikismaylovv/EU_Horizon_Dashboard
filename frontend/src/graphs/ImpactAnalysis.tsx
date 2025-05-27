import React, { useState, useEffect } from 'react'
import axios from 'axios'
import Plot from 'react-plotly.js'

// Define the interface for the impact analysis data
interface ImpactAnalysisData {
  id: number
  title: string
  acronym?: string | null
  total_cost?: number | null
  ec_max_contribution?: number | null
  total_cost_per_year?: number | null
  ec_contribution_per_year?: number | null
  n_institutions?: number | null
  n_publications?: number | null
  duration_days?: number | null
  duration_years?: number | null
  duration_months_remainder?: number | null
  niche?: string | null
  framework_programme?: string | null
}

interface ImpactAnalysisResponse {
  chart_title: string
  data: ImpactAnalysisData[]
  metrics_options: string[]
  x_axis_label: string
  y_axis_label: string
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
  const [plotData, setPlotData] = useState<ImpactAnalysisResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [xMetric, setXMetric] = useState<typeof metricsList[number]>('total_cost')
  const [yMetric, setYMetric] = useState<typeof metricsList[number]>('ec_max_contribution')
  const [xAxisType, setXAxisType] = useState<'linear' | 'log'>('linear')
  const [yAxisType, setYAxisType] = useState<'linear' | 'log'>('linear')

  useEffect(() => {
    (async () => {
      try {
        const endpoint =
          process.env.NODE_ENV === 'development'
            ? '/analytics/impact-analysis' // CRA will proxy this to http://
            : '/api/analytics/impact-analysis' // Vercel will rewrite this to your catch-all
        const res = await axios.get<ImpactAnalysisResponse>(endpoint)
        setPlotData(res.data)
      } catch (err) {
        console.error(err)
        setError('Failed to load impact analysis data')
      } finally {
        setLoading(false)
      }
    })()
  }, [])

  if (loading) return <p>Loading…</p>
  if (error) return <p style={{ color: 'red' }}>{error}</p>
  if (!plotData || !plotData.data || plotData.data.length === 0) return <p>No impact analysis data available.</p>

  const data = plotData.data

  // Prepare data for Plotly
  const getMetricArray = (metric: string) =>
    data.map((d: ImpactAnalysisData) => {
      const val = (d as any)[metric]
      if (typeof val === 'string') {
        // Try to parse as float, fallback to null if not possible
        const parsed = parseFloat(val)
        return isNaN(parsed) ? null : parsed
      }
      return val !== undefined ? val : null
    })

  const customData = data.map((d: ImpactAnalysisData) =>
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

  // Scale type selectors
  const scaleSelector = (
    axis: 'x' | 'y',
    value: 'linear' | 'log',
    setValue: React.Dispatch<React.SetStateAction<'linear' | 'log'>>
  ) => (
    <label style={{ marginRight: 16 }}>
      {axis.toUpperCase()}-scale:&nbsp;
      <select value={value} onChange={e => setValue(e.target.value as 'linear' | 'log')}>
        <option value="linear">Linear</option>
        <option value="log">Log</option>
      </select>
    </label>
  )

  return (
    <div>
      <h2>Impact Analysis</h2>
      <div style={{ marginBottom: 16 }}>
        {axisSelector('x', xMetric, setXMetric)}
        {axisSelector('y', yMetric, setYMetric)}
        <br />
        {scaleSelector('x', xAxisType, setXAxisType)}
        {scaleSelector('y', yAxisType, setYAxisType)}
      </div>
      <Plot
        data={[
          {
            x: getMetricArray(xMetric),
            y: getMetricArray(yMetric),
            text: data.map((d: ImpactAnalysisData) => d.title),
            customdata: customData,
            type: 'scattergl',
            mode: 'markers',
            marker: { size: 7, opacity: 0.7, color: '#636efa' },
            hovertemplate: hoverTemplate,
            hovertext: data.map((d: ImpactAnalysisData) => d.title)
          }
        ]}
        layout={{
          title: { text: 'Impact analysis' },
          xaxis: { 
            title: { text: metricsLabels[xMetric] },
            type: xAxisType
          },
          yaxis: { 
            title: { text: metricsLabels[yMetric] },
            type: yAxisType
          },
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

