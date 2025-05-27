export interface SunburstAPIData {
  labels: string[];
  parents: string[];
  values: (number | null)[];
  metric_name: string;
  max_level_processed: number;
}