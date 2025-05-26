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
        const response = await fetch('http://localhost:8000/analytics/topic-evolution');
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
      stackgroup: 'one',
      name: topic,
      fill: 'tonexty' as const,
      fillcolor: colors[index % colors.length],
      line: { color: colors[index % colors.length] },
      hovertemplate: `<b>${topic}</b><br>Year: %{x}<br>Projects: %{y}<extra></extra>`
    };
  });

  return (
    <div className="bg-white p-6 rounded-lg shadow-lg">
      <h3 className="text-xl font-semibold mb-4">{data.chart_title}</h3>
      
      <div className="h-96 mb-4">
        <Plot
          data={plotData}
          layout={{
            title: { text: '' },
            xaxis: { title: { text: 'Year' } },
            yaxis: { title: { text: 'Number of Projects' } },
            hovermode: 'closest',
            showlegend: true,
            legend: { orientation: 'h', y: -0.2 },
            margin: { l: 50, r: 50, t: 50, b: 100 }
          }}
          config={{ responsive: true }}
          style={{ width: '100%', height: '100%' }}
        />
      </div>

      {/* Summary table */}
      <div className="mt-6">
        <h4 className="text-lg font-medium mb-3">Topic Summary</h4>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {topics.slice(0, 6).map((topic) => {
            const topicData = data.data.filter(d => d.topic === topic);
            const totalProjects = topicData.reduce((sum, d) => sum + d.project_count, 0);
            const totalFunding = topicData.reduce((sum, d) => sum + d.funding_amount, 0);
            
            return (
              <div key={topic} className="bg-gray-50 p-3 rounded">
                <h5 className="font-medium text-sm mb-2 truncate" title={topic}>{topic}</h5>
                <div className="text-xs text-gray-600">
                  <div>Total Projects: {totalProjects}</div>
                  <div>Total Funding: €{totalFunding.toLocaleString()}</div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
};

export default TopicEvolution;
