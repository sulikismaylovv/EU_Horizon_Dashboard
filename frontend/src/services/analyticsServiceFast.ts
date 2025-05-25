// src/services/analyticsService.ts
// src/services/analyticsServiceFast.ts
import axios from 'axios'; // This import statement makes it a module

// Ensure REACT_APP_API_URL is defined in your .env file for your React app
const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000/api/v1';

// Exporting the interface
export interface SunburstData {
  labels: string[];
  parents: string[];
  values: number[];
  metric_name: string;
}

// Exporting the list of available metrics (this was undefined)
export const availableMetrics: string[] = [
    'total_cost',
    'ec_max_contribution',
    'total_cost_per_year',
    'ec_contribution_per_year',
    'duration_days',
    'duration_months',
    'duration_years',
    'n_institutions'
];

// Exporting the data fetching function
export const fetchSunburstChartData = async (metric: string): Promise<SunburstData | null> => {
  try {
    const response = await axios.get<SunburstData>(`${API_BASE_URL}/analytics/sunburst-data`, {
      params: { metric },
    });
    // Handle 204 No Content if the API might return it for valid requests with no data
    if (response.status === 204) {
        return null;
    }
    return response.data;
  } catch (error) {
      console.error(`Error fetching sunburst data):`);
      throw new Error(`Failed to fetch data for metric ${metric}`);

};
}