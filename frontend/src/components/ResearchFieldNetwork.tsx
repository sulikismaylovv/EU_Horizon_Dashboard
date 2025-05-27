import React, { useState, useEffect, useMemo } from 'react';
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
  const [viewMode, setViewMode] = useState<'network' | 'sunburst' | 'ranking'>('ranking');
  const [minProjects, setMinProjects] = useState(10);
  const [maxNodes, setMaxNodes] = useState(50);
  const [selectedMetric, setSelectedMetric] = useState<'size' | 'funding' | 'publications'>('size');

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('http://54.93.51.85:8000/analytics/research-field-network');
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

  // Filter and process data based on user selections
  const processedData = useMemo(() => {
    if (!data) return null;

    // Filter nodes based on minimum project count and exclude "other"
    const filteredNodes = data.nodes
      .filter(node => node.size >= minProjects && node.id !== 'other')
      .sort((a, b) => b[selectedMetric] - a[selectedMetric])
      .slice(0, maxNodes);

    // Create a set of filtered node IDs for edge filtering
    const nodeIds = new Set(filteredNodes.map(n => n.id));

    // Filter edges to only include connections between filtered nodes
    const filteredEdges = data.edges.filter(edge => 
      nodeIds.has(edge.source) && nodeIds.has(edge.target)
    );

    return {
      nodes: filteredNodes,
      edges: filteredEdges
    };
  }, [data, minProjects, maxNodes, selectedMetric]);

  if (loading) return <div className="flex justify-center items-center h-64">Loading...</div>;
  if (error) return <div className="text-red-500 text-center">Error: {error}</div>;
  if (!data || !data.nodes.length) return <div className="text-center">No data available</div>;
  if (!processedData) return <div className="text-center">No data available</div>;

  // Generate circular layout for network visualization
  const generateCircularLayout = () => {
    const nodes = processedData.nodes;
    const positions: { [key: string]: { x: number; y: number } } = {};
    
    const centerX = 400;
    const centerY = 300;
    const radius = 200;
    
    nodes.forEach((node, index) => {
      const angle = (2 * Math.PI * index) / nodes.length;
      positions[node.id] = {
        x: centerX + radius * Math.cos(angle),
        y: centerY + radius * Math.sin(angle)
      };
    });

    return positions;
  };

  // Create network visualization
  const createNetworkVisualization = (): any => {
    const positions = generateCircularLayout();
    const nodes = processedData.nodes;
    const edges = processedData.edges;

    // Edge traces
    const edge_x: (number | null)[] = [];
    const edge_y: (number | null)[] = [];
    
    edges.forEach(edge => {
      const sourcePos = positions[edge.source];
      const targetPos = positions[edge.target];
      if (sourcePos && targetPos) {
        edge_x.push(sourcePos.x, targetPos.x, null);
        edge_y.push(sourcePos.y, targetPos.y, null);
      }
    });

    const edgeTrace = {
      x: edge_x,
      y: edge_y,
      mode: 'lines' as const,
      line: { width: 0.5, color: '#888' },
      hoverinfo: 'none' as const,
      type: 'scatter' as const,
      showlegend: false
    };

    // Node trace
    const nodeTrace = {
      x: nodes.map(node => positions[node.id].x),
      y: nodes.map(node => positions[node.id].y),
      mode: 'markers+text' as any,
      marker: {
        size: nodes.map(node => Math.max(10, Math.min(30, node.size / 5))),
        color: nodes.map(node => node[selectedMetric]),
        colorscale: 'Viridis' as const,
        showscale: true,
        colorbar: {
          title: { 
            text: selectedMetric === 'size' ? 'Projects' : 
                  selectedMetric === 'funding' ? 'Funding (€)' : 'Publications'
          },
          thickness: 15,
          len: 0.7
        } as any,
        line: { width: 2, color: 'white' }
      },
      text: nodes.map(node => {
        const label = node.label.replace(/[\[\]"']/g, '').trim();
        return label.length > 20 ? label.substring(0, 17) + '...' : label;
      }),
      textposition: 'middle center' as const,
      textfont: { size: 8, color: 'white' },
      hovertemplate: '<b>%{text}</b><br>' +
                    'Projects: %{customdata[0]}<br>' +
                    'Publications: %{customdata[1]}<br>' +
                    'Funding: €%{customdata[2]:,.0f}<extra></extra>',
      customdata: nodes.map(node => [node.size, node.publications, node.funding]),
      type: 'scatter' as const,
      showlegend: false
    };

    return [edgeTrace, nodeTrace];
  };

  // Create ranking visualization (horizontal bar chart)
  const createRankingVisualization = (): any => {
    const topFields = processedData.nodes.slice(0, 20);

    return [{
      x: topFields.map(n => n[selectedMetric]),
      y: topFields.map(n => {
        const label = n.label.replace(/[\[\]"']/g, '').trim();
        return label.length > 40 ? label.substring(0, 37) + '...' : label;
      }).reverse(),
      type: 'bar' as const,
      orientation: 'h' as const,
      marker: {
        color: topFields.map(n => n.funding).reverse(),
        colorscale: 'Viridis' as const,
        showscale: false
      },
      hovertemplate: '<b>%{y}</b><br>' +
                    'Projects: %{customdata[0]}<br>' +
                    'Publications: %{customdata[1]}<br>' +
                    'Funding: €%{customdata[2]:,.0f}<extra></extra>',
      customdata: topFields.map(n => [n.size, n.publications, n.funding]).reverse()
    }];
  };

  // Create sunburst visualization
  const createSunburstVisualization = (): any => {
    const nodes = processedData.nodes.slice(0, 30); // Limit for readability
    
    return [{
      type: 'sunburst' as const,
      labels: nodes.map(n => {
        const label = n.label.replace(/[\[\]"']/g, '').trim();
        return label.length > 25 ? label.substring(0, 22) + '...' : label;
      }),
      parents: nodes.map(() => ''),
      values: nodes.map(n => n[selectedMetric]),
      hovertemplate: '<b>%{label}</b><br>' +
                    'Projects: %{customdata[0]}<br>' +
                    'Publications: %{customdata[1]}<br>' +
                    'Funding: €%{customdata[2]:,.0f}<extra></extra>',
      customdata: nodes.map(n => [n.size, n.publications, n.funding]),
      maxdepth: 2
    }];
  };

  const getVisualizationData = (): any => {
    switch (viewMode) {
      case 'network':
        return createNetworkVisualization();
      case 'sunburst':
        return createSunburstVisualization();
      case 'ranking':
        return createRankingVisualization();
      default:
        return createRankingVisualization();
    }
  };

  const getLayoutConfig = () => {
    const baseLayout = {
      margin: { l: viewMode === 'ranking' ? 200 : 50, r: 50, t: 50, b: 50 },
      font: { size: 12 },
      showlegend: false
    };

    switch (viewMode) {
      case 'network':
        return {
          ...baseLayout,
          title: { text: 'Research Field Network', font: { size: 16 } },
          xaxis: { 
            showgrid: false, 
            zeroline: false, 
            showticklabels: false,
            range: [0, 800]
          },
          yaxis: { 
            showgrid: false, 
            zeroline: false, 
            showticklabels: false,
            range: [0, 600]
          },
          plot_bgcolor: 'rgba(0,0,0,0)',
          paper_bgcolor: 'rgba(0,0,0,0)'
        };
      case 'ranking':
        return {
          ...baseLayout,
          title: { text: `Top Research Fields by ${selectedMetric === 'size' ? 'Project Count' : selectedMetric === 'funding' ? 'Funding' : 'Publications'}`, font: { size: 16 } },
          xaxis: { 
            title: { 
              text: selectedMetric === 'size' ? 'Number of Projects' : 
                    selectedMetric === 'funding' ? 'Total Funding (€)' : 'Total Publications'
            }
          },
          yaxis: { title: { text: '' }, automargin: true }
        };
      case 'sunburst':
        return {
          ...baseLayout,
          title: { text: 'Research Field Distribution', font: { size: 16 } }
        };
      default:
        return baseLayout;
    }
  };

  return (
    <div className="bg-white p-6 rounded-lg shadow-lg">
      <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-6 gap-4">
        <h3 className="text-xl font-semibold">{data.chart_title}</h3>
        
        {/* Controls */}
        <div className="flex flex-wrap gap-3 text-sm">
          {/* View Mode Selector */}
          <div className="flex flex-col gap-1">
            <label className="text-xs font-medium text-gray-600">View</label>
            <select 
              value={viewMode} 
              onChange={(e) => setViewMode(e.target.value as any)}
              className="px-2 py-1 border border-gray-300 rounded text-sm"
            >
              <option value="ranking">Ranking</option>
              <option value="network">Network</option>
              <option value="sunburst">Sunburst</option>
            </select>
          </div>

          {/* Metric Selector */}
          <div className="flex flex-col gap-1">
            <label className="text-xs font-medium text-gray-600">Metric</label>
            <select 
              value={selectedMetric} 
              onChange={(e) => setSelectedMetric(e.target.value as any)}
              className="px-2 py-1 border border-gray-300 rounded text-sm"
            >
              <option value="size">Projects</option>
              <option value="funding">Funding</option>
              <option value="publications">Publications</option>
            </select>
          </div>

          {/* Min Projects Filter */}
          <div className="flex flex-col gap-1">
            <label className="text-xs font-medium text-gray-600">Min Projects</label>
            <select 
              value={minProjects} 
              onChange={(e) => setMinProjects(Number(e.target.value))}
              className="px-2 py-1 border border-gray-300 rounded text-sm"
            >
              <option value={5}>5+</option>
              <option value={10}>10+</option>
              <option value={20}>20+</option>
              <option value={50}>50+</option>
            </select>
          </div>

          {/* Max Nodes Selector */}
          {viewMode === 'network' && (
            <div className="flex flex-col gap-1">
              <label className="text-xs font-medium text-gray-600">Max Fields</label>
              <select 
                value={maxNodes} 
                onChange={(e) => setMaxNodes(Number(e.target.value))}
                className="px-2 py-1 border border-gray-300 rounded text-sm"
              >
                <option value={20}>20</option>
                <option value={30}>30</option>
                <option value={50}>50</option>
                <option value={100}>100</option>
              </select>
            </div>
          )}
        </div>
      </div>

      {/* Main Visualization */}
      <div className="mb-4">
        <div className={`${viewMode === 'ranking' ? 'h-96' : 'h-80'} border border-gray-200 rounded`}>
          <Plot
            data={getVisualizationData()}
            layout={getLayoutConfig()}
            config={{ responsive: true, displayModeBar: false }}
            style={{ width: '100%', height: '100%' }}
          />
        </div>
      </div>

      {/* Statistics and Description */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
        <div className="bg-gray-50 p-3 rounded">
          <div className="text-gray-600">Total Fields</div>
          <div className="text-xl font-semibold">{processedData.nodes.length}</div>
        </div>
        <div className="bg-gray-50 p-3 rounded">
          <div className="text-gray-600">Total Projects</div>
          <div className="text-xl font-semibold">
            {processedData.nodes.reduce((sum, node) => sum + node.size, 0).toLocaleString()}
          </div>
        </div>
        <div className="bg-gray-50 p-3 rounded">
          <div className="text-gray-600">Total Funding</div>
          <div className="text-xl font-semibold">
            €{(processedData.nodes.reduce((sum, node) => sum + node.funding, 0) / 1e9).toFixed(1)}B
          </div>
        </div>
      </div>

      <div className="mt-4 text-sm text-gray-600">
        {viewMode === 'network' && (
          <p>Network shows relationships between research fields. Node size indicates {selectedMetric}, and connections show shared research areas.</p>
        )}
        {viewMode === 'ranking' && (
          <p>Ranking shows top research fields ordered by {selectedMetric === 'size' ? 'project count' : selectedMetric}. Use filters to explore different aspects.</p>
        )}
        {viewMode === 'sunburst' && (
          <p>Sunburst chart shows the relative distribution of research fields by {selectedMetric === 'size' ? 'project count' : selectedMetric}.</p>
        )}
      </div>
    </div>
  );
};

export default ResearchFieldNetwork;
