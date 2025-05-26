import React, { useState, useEffect } from 'react';

interface SeasonalityData {
  month: number;
  month_name: string;
  project_starts: number;
  avg_funding: number;
}

interface SeasonalityResponse {
  chart_title: string;
  data: SeasonalityData[];
  chart_type: string;
}

const ProjectSeasonality: React.FC = () => {
  const [data, setData] = useState<SeasonalityResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [metric, setMetric] = useState<'project_starts' | 'avg_funding'>('project_starts');

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('http://localhost:8000/analytics/project-seasonality');
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

  const maxValue = Math.max(...data.data.map(d => d[metric]));

  // Create a simple circular visualization
  const CircularChart = () => {
    const centerX = 150;
    const centerY = 150;
    const radius = 100;
    
    return (
      <div className="flex justify-center">
        <svg width="300" height="300" className="overflow-visible">
          {/* Background circle */}
          <circle cx={centerX} cy={centerY} r={radius} fill="none" stroke="#e5e7eb" strokeWidth="2" />
          
          {/* Month segments */}
          {data.data.map((monthData, index) => {
            const angle = (index * 360 / 12) - 90; // Start from top
            const nextAngle = ((index + 1) * 360 / 12) - 90;
            const value = monthData[metric];
            const normalizedValue = (value / maxValue) * radius;
            
            // Calculate positions
            const x1 = centerX + Math.cos(angle * Math.PI / 180) * radius;
            const y1 = centerY + Math.sin(angle * Math.PI / 180) * radius;
            const x2 = centerX + Math.cos(angle * Math.PI / 180) * normalizedValue;
            const y2 = centerY + Math.sin(angle * Math.PI / 180) * normalizedValue;
            
            // Label position
            const labelX = centerX + Math.cos(angle * Math.PI / 180) * (radius + 20);
            const labelY = centerY + Math.sin(angle * Math.PI / 180) * (radius + 20);
            
            return (
              <g key={monthData.month}>
                {/* Radial line */}
                <line x1={centerX} y1={centerY} x2={x1} y2={y1} stroke="#e5e7eb" strokeWidth="1" />
                {/* Value line */}
                <line x1={centerX} y1={centerY} x2={x2} y2={y2} stroke="#3b82f6" strokeWidth="3" />
                {/* Value point */}
                <circle cx={x2} cy={y2} r="4" fill="#3b82f6" />
                {/* Month label */}
                <text x={labelX} y={labelY} textAnchor="middle" fontSize="12" fill="#374151">
                  {monthData.month_name}
                </text>
              </g>
            );
          })}
          
          {/* Center label */}
          <text x={centerX} y={centerY} textAnchor="middle" fontSize="14" fontWeight="bold" fill="#374151">
            {metric === 'project_starts' ? 'Projects' : 'Funding'}
          </text>
        </svg>
      </div>
    );
  };

  return (
    <div className="bg-white p-6 rounded-lg shadow-lg">
      <div className="flex justify-between items-center mb-4">
        <h3 className="text-xl font-semibold">{data.chart_title}</h3>
        <select 
          value={metric}
          onChange={(e) => setMetric(e.target.value as 'project_starts' | 'avg_funding')}
          className="border rounded px-3 py-1"
        >
          <option value="project_starts">Project Starts</option>
          <option value="avg_funding">Average Funding</option>
        </select>
      </div>

      <CircularChart />

      {/* Data table */}
      <div className="mt-6 overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Month
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Project Starts
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Average Funding (€)
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {data.data.map((item) => (
              <tr key={item.month} className="hover:bg-gray-50">
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                  {item.month_name}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {item.project_starts}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {item.avg_funding.toLocaleString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default ProjectSeasonality;
