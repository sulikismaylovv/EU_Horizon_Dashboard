export interface Project {
  id: number; // bigint not null
  acronym: string; // text not null
  status?: string | null;
  title?: string | null;
  start_date?: string | null; // ISO date string (YYYY-MM-DD)
  end_date?: string | null; // ISO date string (YYYY-MM-DD)
  total_cost?: number | null; // numeric
  ec_max_contribution?: number | null; // numeric
  ec_signature_date?: string | null; // ISO date string
  framework_programme?: string | null;
  master_call?: string | null;
  sub_call?: string | null;
  funding_scheme?: string | null;
  nature?: string | null;
  objective?: string | null;
  content_update_date?: string | null; // ISO datetime string (timestamp with time zone)
  rcn?: string | null; // text, unique
  grant_doi?: string | null;
  duration_days?: number | null;
  duration_months?: number | null;
  duration_years?: number | null;
  n_institutions?: number | null;
  coordinator_name?: string | null;
  ec_contribution_per_year?: number | null; // numeric
  total_cost_per_year?: number | null; // numeric
  field_class?: string | null;
  field?: string | null;
  sub_field?: string | null;
  niche?: string | null;
}
