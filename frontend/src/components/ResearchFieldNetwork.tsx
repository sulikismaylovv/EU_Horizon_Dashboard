import React, { useState, useEffect } from 'react';
import Plot from 'react-plotly.js';

interface ResearchFieldNetworkNode {
  id: string;
  label: string;
  size: number;
  publications: number;
  funding: number;
}

interface NetworkEdge {
  source: string;
  target: string;
  weight: number;
}

interface ResearchFieldNetworkResponse {
  chart_title: string;
  nodes: ResearchFieldNetworkNode[];
  edges: NetworkEdge[];
}

const ResearchFieldNetwork: React.FC = () => {
  const [data, setData] = useState<ResearchFieldNetworkResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('http://localhost:8000/analytics/research-field-network');
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
  if (!data || !data.nodes.length) return <div className="text-center">No data available</div>;

  // Create sunburst chart data for research fields
  const sunburstData = [{
    type: 'sunburst' as const,
    labels: data.nodes.map(n => n.label),
    parents: data.nodes.map(() => ''), // All are root level for now
    values: data.nodes.map(n => n.size),
    hovertemplate: '<b>%{label}</b><br>Projects: %{value}<br>Publications: %{customdata[0]}<br>Funding: €%{customdata[1]:,.0f}<extra></extra>',
    customdata: data.nodes.map(n => [n.publications, n.funding]),
    maxdepth: 2
  }];

  // Bar chart of top fields
  const barData = [{
    x: data.nodes.slice(0, 10).map(n => n.size),
    y: data.nodes.slice(0, 10).map(n => n.label),
    type: 'bar' as const,
    orientation: 'h' as const,
    marker: {
      color: data.nodes.slice(0, 10).map(n => n.funding),
      colorscale: 'Viridis' as const,
      showscale: true,
      colorbar: {
        title: {
          text: 'Total Funding (€)'
        }
      }
    },
    hovertemplate: '<b>%{y}</b><br>Projects: %{x}<br>Funding: €%{marker.color:,.0f}<extra></extra>'
  }];

  return (
    <div className="bg-white p-6 rounded-lg shadow-lg">
      <h3 className="text-xl font-semibold mb-4">{data.chart_title}</h3>
      
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        {/* Sunburst chart */}
        <div>
          <h4 className="text-lg font-medium mb-3">Research Field Distribution</h4>
          <div className="h-80">
            <Plot
              data={sunburstData}
              layout={{
                title: { text: '' },
                margin: { l: 0, r: 0, t: 0, b: 0 },
                showlegend: false
              }}
              config={{ responsive: true }}
              style={{ width: '100%', height: '100%' }}
            />
          </div>
        </div>

        {/* Bar chart */}
        <div>
          <h4 className="text-lg font-medium mb-3">Top Research Fields by Project Count</h4>
          <div className="h-80">
            <Plot
              data={barData}
              layout={{
                title: { text: '' },
                xaxis: { title: { text: 'Number of Projects' } },
                yaxis: { title: { text: '' }, automargin: true },
                margin: { l: 150, r: 50, t: 20, b: 50 }
              }}
              config={{ responsive: true }}
              style={{ width: '100%', height: '100%' }}
            />
          </div>
        </div>
      </div>

      {/* Field statistics */}
      <div className="mt-6">
        <h4 className="text-lg font-medium mb-3">Field Statistics</h4>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {data.nodes.slice(0, 6).map((node) => (
            <div key={node.id} className="bg-gray-50 p-4 rounded-lg">
              <h5 className="font-semibold text-gray-900 mb-2 truncate" title={node.label}>
                {node.label}
              </h5>
              
              <div className="space-y-1 text-sm text-gray-600">
                <div className="flex justify-between">
                  <span>Projects:</span>
                  <span className="font-medium">{node.size}</span>
                </div>
                <div className="flex justify-between">
                  <span>Publications:</span>
                  <span className="font-medium">{node.publications}</span>
                </div>
                <div className="flex justify-between">
                  <span>Funding:</span>
                  <span className="font-medium">€{node.funding.toLocaleString()}</span>
                </div>
                <div className="flex justify-between">
                  <span>Avg per Project:</span>
                  <span className="font-medium">€{(node.funding / node.size).toLocaleString()}</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default ResearchFieldNetwork;
