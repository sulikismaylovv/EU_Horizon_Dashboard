import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { SunburstAPIData } from './interfaces/Sunburst';

const SunburstDataViewer: React.FC = () => {
  const [data, setData] = useState<SunburstAPIData | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        const response = await axios.get<SunburstAPIData>('http://127.0.0.1:8000/projects/analytics/sunburst');
        setData(response.data);
      } catch (err) {

          setError('An unexpected error occurred.');
        
        console.error("Error fetching data:", err);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []); // Empty dependency array means this effect runs once on mount

  if (loading) {
    return <p>Loading data...</p>;
  }

  if (error) {
    return <p style={{ color: 'red' }}>{error}</p>;
  }

  if (!data) {
    return <p>No data available.</p>;
  }

  return (
    <div>
      <h2>Raw Sunburst Data:</h2>
      <pre style={{ border: '1px solid #ccc', padding: '10px', backgroundColor: '#f9f9f9', whiteSpace: 'pre-wrap', wordWrap: 'break-word' }}>
        {JSON.stringify(data, null, 2)}
      </pre>
    </div>
  );
};

export default SunburstDataViewer;