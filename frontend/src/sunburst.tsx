// src/components/SunburstChartComponent.tsx
import React, { useState, useEffect } from 'react';
import { fetchSunburstChartData, SunburstData, availableMetrics } from './services/analyticsServiceFast'; // Adjust the import path as necessary

// Dynamically import Plotly to potentially reduce initial bundle size
const Plot = React.lazy(() =>
  import('react-plotly.js').then(module => ({ default: module.default }))
);
const SunburstChartComponent: React.FC = () => {
  const [chartData, setChartData] = useState<SunburstData | null>(null);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedMetric, setSelectedMetric] = useState<string>(availableMetrics[1]); // Default to ec_max_contribution

  useEffect(() => {
    const loadData = async () => {
      setIsLoading(true);
      setError(null);
      setChartData(null); // Clear previous data
      try {
        const data = await fetchSunburstChartData(selectedMetric);
        if (data && data.labels && data.labels.length > 0) {
            setChartData(data);
        } else {
            setError(`No data available for metric: ${selectedMetric}.`);
        }
      } catch (err: any) {
        setError(err.message || 'Failed to load chart data.');
      } finally {
        setIsLoading(false);
      }
    };

    loadData();
  }, [selectedMetric]);

  const handleMetricChange = (event: React.ChangeEvent<HTMLSelectElement>) => {
    setSelectedMetric(event.target.value);
  };

  return (
    <div>
      <h2>Project Analytics Sunburst Chart</h2>
      <div>
        <label htmlFor="metric-select">Select Metric: </label>
        <select id="metric-select" value={selectedMetric} onChange={handleMetricChange}>
          {availableMetrics.map(metric => (
            <option key={metric} value={metric}>
              {metric.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())} {/* Prettify metric name */}
            </option>
          ))}
        </select>
      </div>

      {isLoading && <p>Loading chart data...</p>}
      {error && <p style={{ color: 'red' }}>Error: {error}</p>}
      
      {!isLoading && !error && chartData && chartData.labels.length > 0 && (
        // <Suspense fallback={<div>Loading Chart Library...</div>}>
          <Plot
            data={[
              {
                type: 'sunburst',
                labels: chartData.labels,
                parents: chartData.parents,
                values: chartData.values,
                // @ts-ignore: Plotly sunburst supports outsidetextfont but typings may not
                outsidetextfont: { size: 20, color: '#377eb8' },
                leaf: { opacity: 0.8 }, // Make leaves slightly more distinct
                marker: { line: { width: 1.5 } }, // Add lines between segments
                branchvalues: 'total', // As in your Python script
                insidetextorientation: 'radial', // As in your Python script
                maxdepth: 3, // As in your Python script, or make it dynamic
              },
            ]}
            layout={{
              margin: { l: 10, r: 10, b: 10, t: 30 }, // Adjusted margins
              width: 700,
              height: 700,
              title: { text: `Sunburst: ${selectedMetric.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}` },
              // You can explore more layout options from Plotly documentation
            }}
            config={{
                responsive: true, // Makes the chart responsive
                displaylogo: false, // Hides the Plotly logo
            }}
          />
        // </Suspense>
      )}
      {!isLoading && !error && (!chartData || chartData.labels.length === 0) && !isLoading && (
         <p>No data to display for the selected metric.</p>
      )}
    </div>
  );
};

export default SunburstChartComponent;