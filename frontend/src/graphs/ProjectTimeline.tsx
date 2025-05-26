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

const ProjectTimeline: React.FC = () => {
  const [plotData, setPlotData] = useState<ProjectTimelineResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [metric, setMetric] = useState('project_count')

  const fetchData = async (selectedMetric: string) => {
    try {
      setLoading(true)
      const res = await axios.get<ProjectTimelineResponse>(
        `http://127.0.0.1:8000/analytics/project-timeline?metric=${selectedMetric}`
      )
      setPlotData(res.data)
    } catch (err) {
      console.error(err)
      setError('Failed to load project timeline data')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData(metric)
  }, [metric])

  const handleMetricChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
    setMetric(event.target.value)
  }

  if (loading) return <p>Loading…</p>
  if (error) return <p style={{ color: 'red' }}>{error}</p>
  if (!plotData || plotData.data.length === 0) return <p>No timeline data available.</p>

  const years = plotData.data.map(item => item.year)
  const values = plotData.data.map(item => item.metric_value)

  return (
    <div>
      <div style={{ marginBottom: '20px' }}>
        <label htmlFor="metric-select">Select Metric: </label>
        <select id="metric-select" value={metric} onChange={handleMetricChange}>
          <option value="project_count">Project Count</option>
          <option value="total_funding">Total Funding</option>
          <option value="average_duration">Average Duration</option>
        </select>
      </div>
      <Plot
        data={[
          {
            type: 'scatter',
            mode: 'lines+markers',
            x: years,
            y: values,
            line: { color: 'rgb(55, 128, 191)', width: 3 },
            marker: { size: 8, color: 'rgb(55, 128, 191)' },
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

export default ProjectTimeline
