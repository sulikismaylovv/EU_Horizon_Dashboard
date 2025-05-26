import React, { useState, useEffect } from 'react';
import Plot from 'react-plotly.js';

interface PublicationAnalysis {
  year: number;
  publication_count: number;
  projects_with_publications: number;
  avg_publications_per_project: number;
}

interface PublicationTimelineResponse {
  chart_title: string;
  x_axis_label: string;
  y_axis_label: string;
  data: PublicationAnalysis[];
}

const ResearchOutputTimeline: React.FC = () => {
  const [data, setData] = useState<PublicationTimelineResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('http://localhost:8000/analytics/research-output-timeline');
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const result = await response.json();
        setData(result);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'An error occurred');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  if (loading) return <div className="flex justify-center items-center h-64">Loading...</div>;
  if (error) return <div className="text-red-500 text-center">Error: {error}</div>;
  if (!data || !data.data.length) return <div className="text-center">No data available</div>;

  const years = data.data.map(d => d.year);
  
  const plotData = [
    {
      x: years,
      y: data.data.map(d => d.publication_count),
      type: 'scatter' as const,
      mode: 'lines+markers' as const,
      name: 'Total Publications',
      line: { color: '#1f77b4', width: 3 },
      marker: { size: 8 },
      hovertemplate: 'Year: %{x}<br>Publications: %{y}<extra></extra>'
    },
    {
      x: years,
      y: data.data.map(d => d.projects_with_publications),
      type: 'scatter' as const,
      mode: 'lines+markers' as const,
      name: 'Projects with Publications',
      line: { color: '#ff7f0e', width: 3 },
      marker: { size: 8 },
      hovertemplate: 'Year: %{x}<br>Projects: %{y}<extra></extra>'
    },
    {
      x: years,
      y: data.data.map(d => d.avg_publications_per_project),
      type: 'scatter' as const,
      mode: 'lines+markers' as const,
      name: 'Avg Publications per Project',
      line: { color: '#2ca02c', width: 3 },
      marker: { size: 8 },
      yaxis: 'y2',
      hovertemplate: 'Year: %{x}<br>Avg per Project: %{y:.2f}<extra></extra>'
    }
  ];

  return (
    <div className="bg-white p-6 rounded-lg shadow-lg">
      <h3 className="text-xl font-semibold mb-4">{data.chart_title}</h3>
      
      <div className="h-96 mb-4">
        <Plot
          data={plotData}
          layout={{
            title: { text: '' },
            xaxis: { title: { text: 'Year' } },
            yaxis: { title: { text: 'Count' }, side: 'left' },
            yaxis2: {
              title: { text: 'Average per Project' },
              side: 'right',
              overlaying: 'y'
            },
            hovermode: 'x unified',
            showlegend: true,
            legend: { orientation: 'h', y: -0.2 },
            margin: { l: 50, r: 50, t: 50, b: 100 }
          }}
          config={{ responsive: true }}
          style={{ width: '100%', height: '100%' }}
        />
      </div>

      {/* Summary stats */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-blue-50 p-4 rounded-lg text-center">
          <h4 className="text-lg font-semibold text-blue-800">Total Publications</h4>
          <p className="text-2xl font-bold text-blue-600">
            {data.data.reduce((sum, d) => sum + d.publication_count, 0).toLocaleString()}
          </p>
        </div>
        <div className="bg-green-50 p-4 rounded-lg text-center">
          <h4 className="text-lg font-semibold text-green-800">Active Projects</h4>
          <p className="text-2xl font-bold text-green-600">
            {Math.max(...data.data.map(d => d.projects_with_publications))}
          </p>
        </div>
        <div className="bg-purple-50 p-4 rounded-lg text-center">
          <h4 className="text-lg font-semibold text-purple-800">Peak Efficiency</h4>
          <p className="text-2xl font-bold text-purple-600">
            {Math.max(...data.data.map(d => d.avg_publications_per_project)).toFixed(1)}
          </p>
        </div>
      </div>
    </div>
  );
};

export default ResearchOutputTimeline;
