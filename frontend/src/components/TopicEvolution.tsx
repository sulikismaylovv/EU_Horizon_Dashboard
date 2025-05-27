import React, { useState, useEffect } from 'react';
import Plot from 'react-plotly.js';

interface TopicEvolutionData {
  topic: string;
  year: number;
  project_count: number;
  funding_amount: number;
}

interface TopicEvolutionResponse {
  chart_title: string;
  data: TopicEvolutionData[];
  x_axis_label: string;
  y_axis_label: string;
}

const TopicEvolution: React.FC = () => {
  const [data, setData] = useState<TopicEvolutionResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const endpoint = process.env.NODE_ENV === 'development'
          ? '/analytics/topic-evolution' // CRA will proxy this to http://
          : '/api/analytics/topic-evolution'; // Vercel will rewrite this to your catch-all
        const response = await fetch(endpoint);
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

  // Transform data for area chart
  const topics = Array.from(new Set(data.data.map(d => d.topic)));
  const years = Array.from(new Set(data.data.map(d => d.year))).sort();
  
  console.log('Topics found:', topics.length);
  console.log('Years found:', years);
  console.log('Sample data:', data.data.slice(0, 5));
  
  const plotData = topics.slice(0, 8).map((topic, index) => {
    const topicData = data.data.filter(d => d.topic === topic);
    const yearData = years.map(year => {
      const yearEntry = topicData.find(d => d.year === year);
      return yearEntry ? yearEntry.project_count : 0;
    });

    const colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f'];
    
    return {
      x: years,
      y: yearData,
      type: 'scatter' as const,
      mode: 'lines+markers' as const,
      name: topic,
      line: { color: colors[index % colors.length] },
      marker: { color: colors[index % colors.length] }
    };
  });

  console.log('Plot data being sent to Plotly:', plotData);
  console.log('Plot data length:', plotData.length);
  console.log('First plot data item:', plotData[0]);

  return (
    <div className="bg-white p-6 rounded-lg shadow-lg">
      <h3 className="text-xl font-semibold mb-4">{data.chart_title}</h3>
      <div className="mb-4">
        <p>Debug Info:</p>
        <p>Topics: {topics.length}</p>
        <p>Years: {years.join(', ')}</p>
        <p>Plot data items: {plotData.length}</p>
      </div>
      
      <div style={{ width: '100%', height: '400px', border: '1px solid #ccc' }}>
        {plotData.length > 0 ? (
          <Plot
            data={plotData}
            layout={{
              width: undefined,
              height: undefined,
              title: { text: '' },
              xaxis: { 
                title: { text: 'Year' },
                type: 'linear'
              },
              yaxis: { 
                title: { text: 'Number of Projects' },
                type: 'linear'
              },
              hovermode: 'closest',
              showlegend: true,
              legend: { orientation: 'h', y: -0.2 },
              margin: { l: 50, r: 50, t: 20, b: 100 },
              autosize: true
            }}
            config={{ 
              responsive: true, 
              displayModeBar: true,
              displaylogo: false 
            }}
            style={{ width: '100%', height: '100%' }}
            useResizeHandler={true}
            onError={(error) => console.error('Plotly error:', error)}
            onInitialized={() => console.log('Plotly initialized successfully')}
          />
        ) : (
          <div className="flex justify-center items-center h-full">
            <p>No plot data available. Plot data length: {plotData.length}</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default TopicEvolution;
