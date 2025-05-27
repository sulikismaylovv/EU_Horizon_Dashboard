import React, { useState, useEffect, useMemo } from 'react';
import Plot from 'react-plotly.js';

interface NetworkNode {
  id: string;
  label: string;
  size: number;
  group: string;
}

interface NetworkEdge {
  source: string;
  target: string;
  weight: number;
}

interface NetworkGraphResponse {
  chart_title: string;
  nodes: NetworkNode[];
  edges: NetworkEdge[];
  description?: string;
}

const CollaborationNetwork: React.FC = () => {
  const [data, setData] = useState<NetworkGraphResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<'network' | 'ranking' | 'map'>('network');
  const [minCollaborations, setMinCollaborations] = useState(2);
  const [maxNodes, setMaxNodes] = useState(30);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        console.log('Fetching collaboration network data...');
        const response = await fetch(`http://54.93.51.85:8000/analytics/collaboration-network?min_collaborations=${minCollaborations}`);
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const result = await response.json();
        console.log('Collaboration network data received:', result);
        setData(result);
      } catch (err) {
        console.error('Error fetching collaboration network data:', err);
        setError(err instanceof Error ? err.message : 'An error occurred');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [minCollaborations]);

  // Filter and process data based on user selections
  const processedData = useMemo(() => {
    if (!data) return null;

    // Filter nodes based on collaboration count and sort by size
    const filteredNodes = data.nodes
      .sort((a, b) => b.size - a.size)
      .slice(0, maxNodes);

    // Create a set of filtered node IDs for edge filtering
    const nodeIds = new Set(filteredNodes.map(n => n.id));

    // Filter edges to only include connections between filtered nodes
    const filteredEdges = data.edges.filter(edge => 
      nodeIds.has(edge.source) && nodeIds.has(edge.target) && edge.weight >= minCollaborations
    );

    return {
      nodes: filteredNodes,
      edges: filteredEdges
    };
  }, [data, minCollaborations, maxNodes]);

  if (loading) return <div className="flex justify-center items-center h-64">Loading collaboration network...</div>;
  if (error) return <div className="text-red-500 text-center">Error loading collaboration network: {error}</div>;
  if (!data || !data.nodes.length) return <div className="text-center">No collaboration data available</div>;
  if (!processedData) return <div className="text-center">No processed collaboration data available</div>;

  // Generate circular layout for network visualization
  const generateCircularLayout = () => {
    const nodes = processedData.nodes;
    const positions: { [key: string]: { x: number; y: number } } = {};
    
    const centerX = 400;
    const centerY = 300;
    const radius = 220;

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
      line: { width: edges.map(e => Math.min(5, e.weight / 5)), color: '#888' },
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
        size: nodes.map(node => Math.max(12, Math.min(40, node.size / 10))),
        color: nodes.map(node => node.size),
        colorscale: 'Blues' as const,
        showscale: true,
        colorbar: {
          title: { text: 'Project Count' },
          thickness: 15,
          len: 0.7
        } as any,
        line: { width: 2, color: 'white' }
      },
      text: nodes.map(node => node.label),
      textposition: 'middle center' as const,
      textfont: { size: 10, color: 'white', family: 'Arial Black' },
      hovertemplate: '<b>%{text}</b><br>' +
                    'Projects: %{customdata[0]}<br>' +
                    'Collaborations: %{customdata[1]}<extra></extra>',
      customdata: nodes.map(node => {
        const collaborationCount = processedData.edges.filter(
          e => e.source === node.id || e.target === node.id
        ).length;
        return [node.size, collaborationCount];
      }),
      type: 'scatter' as const,
      showlegend: false
    };

    return [edgeTrace, nodeTrace];
  };

  // Create ranking visualization (horizontal bar chart)
  const createRankingVisualization = (): any => {
    const topCountries = processedData.nodes.slice(0, 20);

    return [{
      x: topCountries.map(n => n.size),
      y: topCountries.map(n => n.label).reverse(),
      type: 'bar' as const,
      orientation: 'h' as const,
      marker: {
        color: topCountries.map(n => n.size).reverse(),
        colorscale: 'Blues' as const,
        showscale: false
      },
      hovertemplate: '<b>%{y}</b><br>' +
                    'Projects: %{x}<br>' +
                    'Collaborations: %{customdata}<extra></extra>',
      customdata: topCountries.map(node => {
        const collaborationCount = processedData.edges.filter(
          e => e.source === node.id || e.target === node.id
        ).length;
        return collaborationCount;
      }).reverse()
    }];
  };

  // Create map visualization placeholder
  const createMapVisualization = (): any => {
    // This would be implemented with a proper world map
    // For now, returning a simple scatter plot
    const nodes = processedData.nodes;
    
    return [{
      type: 'scatter' as const,
      mode: 'markers+text' as const,
      x: nodes.map((_, i) => i % 10),
      y: nodes.map((_, i) => Math.floor(i / 10)),
      marker: {
        size: nodes.map(node => Math.max(10, Math.min(30, node.size / 20))),
        color: nodes.map(node => node.size),
        colorscale: 'Blues' as const,
        showscale: true
      },
      text: nodes.map(node => node.label),
      textposition: 'middle center' as const,
      hovertemplate: '<b>%{text}</b><br>Projects: %{customdata}<extra></extra>',
      customdata: nodes.map(node => node.size)
    }];
  };

  const getVisualizationData = (): any => {
    switch (viewMode) {
      case 'network':
        return createNetworkVisualization();
      case 'ranking':
        return createRankingVisualization();
      case 'map':
        return createMapVisualization();
      default:
        return createNetworkVisualization();
    }
  };

  const getLayoutConfig = () => {
    const baseLayout = {
      margin: { l: viewMode === 'ranking' ? 80 : 50, r: 50, t: 50, b: 50 },
      font: { size: 12 },
      showlegend: false
    };

    switch (viewMode) {
      case 'network':
        return {
          ...baseLayout,
          title: { text: 'Country Collaboration Network', font: { size: 16 } },
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
          title: { text: 'Top Countries by Project Count', font: { size: 16 } },
          xaxis: { title: { text: 'Number of Projects' } },
          yaxis: { title: { text: '' }, automargin: true }
        };
      case 'map':
        return {
          ...baseLayout,
          title: { text: 'Country Collaboration Map', font: { size: 16 } },
          xaxis: { showgrid: false, zeroline: false, showticklabels: false },
          yaxis: { showgrid: false, zeroline: false, showticklabels: false }
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
              <option value="network">Network</option>
              <option value="ranking">Ranking</option>
              <option value="map">Map</option>
            </select>
          </div>

          {/* Min Collaborations Filter */}
          <div className="flex flex-col gap-1">
            <label className="text-xs font-medium text-gray-600">Min Collaborations</label>
            <select 
              value={minCollaborations} 
              onChange={(e) => setMinCollaborations(Number(e.target.value))}
              className="px-2 py-1 border border-gray-300 rounded text-sm"
            >
              <option value={1}>1+</option>
              <option value={2}>2+</option>
              <option value={5}>5+</option>
              <option value={10}>10+</option>
            </select>
          </div>

          {/* Max Countries Selector */}
          {viewMode === 'network' && (
            <div className="flex flex-col gap-1">
              <label className="text-xs font-medium text-gray-600">Max Countries</label>
              <select 
                value={maxNodes} 
                onChange={(e) => setMaxNodes(Number(e.target.value))}
                className="px-2 py-1 border border-gray-300 rounded text-sm"
              >
                <option value={20}>20</option>
                <option value={30}>30</option>
                <option value={50}>50</option>
                <option value={100}>All</option>
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
          <div className="text-gray-600">Countries</div>
          <div className="text-xl font-semibold">{processedData.nodes.length}</div>
        </div>
        <div className="bg-gray-50 p-3 rounded">
          <div className="text-gray-600">Collaborations</div>
          <div className="text-xl font-semibold">{processedData.edges.length}</div>
        </div>
        <div className="bg-gray-50 p-3 rounded">
          <div className="text-gray-600">Total Projects</div>
          <div className="text-xl font-semibold">
            {processedData.nodes.reduce((sum, node) => sum + node.size, 0).toLocaleString()}
          </div>
        </div>
      </div>

      <div className="mt-4 text-sm text-gray-600">
        {viewMode === 'network' && (
          <p>Network shows collaboration patterns between countries. Node size indicates project count, edge thickness shows collaboration strength.</p>
        )}
        {viewMode === 'ranking' && (
          <p>Ranking shows countries ordered by total project count. Hover for collaboration details.</p>
        )}
        {viewMode === 'map' && (
          <p>Geographic representation of country participation. Node size represents project involvement.</p>
        )}
      </div>
    </div>
  );
};

export default CollaborationNetwork;
