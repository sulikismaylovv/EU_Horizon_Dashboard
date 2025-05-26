import React, { useState, useEffect } from 'react';
import Plot from 'react-plotly.js';

interface ProjectSuccessMetrics {
  project_acronym: string;
  duration_months: number;
  publications_count: number;
  funding_efficiency: number;
  collaboration_score: number;
}

interface ProjectSuccessResponse {
  chart_title: string;
  data: ProjectSuccessMetrics[];
  x_axis_label: string;
  y_axis_label: string;
}

const ProjectSuccessAnalysis: React.FC = () => {
  const [data, setData] = useState<ProjectSuccessResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('http://localhost:8000/analytics/project-success-metrics');
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
  // Prepare scatter plot data
  const plotData = [{
    x: data.data.map(d => d.publications_count),
    y: data.data.map(d => d.funding_efficiency),
    mode: 'markers' as const,
    type: 'scatter' as const,
    marker: {
      size: data.data.map(d => Math.max(d.collaboration_score * 3, 8)),
      color: data.data.map(d => d.collaboration_score),
      colorscale: 'Viridis' as const,
      showscale: true,
      colorbar: {
        title: {
          text: 'Collaboration Score<br>(# Countries)'
        }
      },
      line: { width: 1, color: 'white' }
    },
    text: data.data.map(d => `${d.project_acronym}<br>Duration: ${d.duration_months}m<br>Countries: ${d.collaboration_score}`),
    hovertemplate: '<b>%{text}</b><br>Publications: %{x}<br>Funding Efficiency: %{y:.2f}<extra></extra>'
  }];

  const getCollaborationBadge = (score: number) => {
    if (score >= 5) return { color: 'bg-orange-100 text-orange-800', text: 'High' };
    if (score >= 3) return { color: 'bg-yellow-100 text-yellow-800', text: 'Medium' };
    return { color: 'bg-blue-100 text-blue-800', text: 'Low' };
  };

  return (
    <div className="bg-white p-6 rounded-lg shadow-lg">
      <h3 className="text-xl font-semibold mb-4">{data.chart_title}</h3>
      
      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <div className="bg-blue-50 p-4 rounded-lg text-center">
          <h4 className="text-lg font-semibold text-blue-800">Total Projects</h4>
          <p className="text-2xl font-bold text-blue-600">{data.data.length}</p>
        </div>
        <div className="bg-green-50 p-4 rounded-lg text-center">
          <h4 className="text-lg font-semibold text-green-800">Avg Publications</h4>
          <p className="text-2xl font-bold text-green-600">
            {(data.data.reduce((sum, p) => sum + p.publications_count, 0) / data.data.length).toFixed(1)}
          </p>
        </div>
        <div className="bg-yellow-50 p-4 rounded-lg text-center">
          <h4 className="text-lg font-semibold text-yellow-800">Avg Efficiency</h4>
          <p className="text-2xl font-bold text-yellow-600">
            {(data.data.reduce((sum, p) => sum + p.funding_efficiency, 0) / data.data.length).toFixed(2)}
          </p>
        </div>
        <div className="bg-purple-50 p-4 rounded-lg text-center">
          <h4 className="text-lg font-semibold text-purple-800">Avg Countries</h4>
          <p className="text-2xl font-bold text-purple-600">
            {(data.data.reduce((sum, p) => sum + p.collaboration_score, 0) / data.data.length).toFixed(1)}
          </p>
        </div>
      </div>

      {/* Scatter plot */}
      <div className="h-96 mb-6">
        <Plot
          data={plotData}
          layout={{
            title: { text: '' },
            xaxis: { title: { text: 'Number of Publications' } },
            yaxis: { title: { text: 'Funding Efficiency (Publications per Million EUR)' } },
            hovermode: 'closest',
            showlegend: false,
            margin: { l: 50, r: 100, t: 50, b: 50 }
          }}
          config={{ responsive: true }}
          style={{ width: '100%', height: '100%' }}
        />
      </div>

      {/* Top performers */}
      <div className="mt-6">
        <h4 className="text-lg font-medium mb-3">Top Performing Projects</h4>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {data.data
            .sort((a, b) => b.funding_efficiency - a.funding_efficiency)
            .slice(0, 6)
            .map((project, index) => {
              const collabBadge = getCollaborationBadge(project.collaboration_score);
              
              return (
                <div key={index} className="border rounded-lg p-4 hover:shadow-md transition-shadow">
                  <h5 className="font-semibold text-gray-900 mb-2 truncate" title={project.project_acronym}>
                    {project.project_acronym}
                  </h5>
                  
                  <div className="space-y-2 text-sm">
                    <div className="flex justify-between">
                      <span className="text-gray-600">Publications:</span>
                      <span className="font-medium">{project.publications_count}</span>
                    </div>
                    
                    <div className="flex justify-between">
                      <span className="text-gray-600">Efficiency:</span>
                      <span className="font-medium">{project.funding_efficiency.toFixed(2)}</span>
                    </div>
                    
                    <div className="flex justify-between items-center">
                      <span className="text-gray-600">Collaboration:</span>
                      <span className={`px-2 py-1 rounded-full text-xs font-medium ${collabBadge.color}`}>
                        {collabBadge.text}
                      </span>
                    </div>
                  </div>
                </div>
              );
            })}
        </div>
      </div>
    </div>
  );
};

export default ProjectSuccessAnalysis;
