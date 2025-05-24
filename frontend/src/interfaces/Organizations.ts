export interface Organization {
  id: number; // bigint not null
  name: string; // text not null
  short_name?: string | null; // text null
  vat_number?: string | null; // text null
  sme?: boolean | null; // boolean null
  activity_type?: string | null; // text null
  street?: string | null; // text null
  post_code?: string | null; // text null
  city?: string | null; // text null
  country?: string | null; // text null
  nuts_code?: string | null; // text null
  geolocation?: string | null; // text null
  organization_url?: string | null; // text null
  contact_form?: string | null; // text null
  content_update_date?: string | null; // timestamp with time zone null (ISO format)
}
