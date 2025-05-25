import React, { useEffect, useState } from 'react';
import { SunburstAPIData } from './interfaces/Sunburst';
// Define an interface for your expected data structure


const MyComponent: React.FC = () => {
  const [data, setData] = useState<SunburstAPIData[] | null>(null); // To store the fetched data
  const [loading, setLoading] = useState<boolean>(true); // To indicate loading state
  const [error, setError] = useState<string | null>(null); // To store any errors

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('http://127.0.0.1:8000/projects/analytics/sunburst');
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const jsonData: SunburstAPIData[] = await response.json();
        setData(jsonData);
      } catch (e) {
        if (e instanceof Error) {
          setError(e.message);
        } else {
          setError('An unknown error occurred');
        }
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []); // The empty dependency array means this effect runs once after the initial render

  if (loading) {
    return <p>Loading...</p>;
  }

  if (error) {
    return <p>Error: {error}</p>;
  }

  return (
    <div>
      <h1>Fetched Data</h1>
      {data && (
        <ul>
          {data.map(post => ( // Displaying all posts
            <li key={post.max_level_processed}>
              <h2>{post.values}</h2>
              <p>{post.labels}</p>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
};

export default MyComponent;