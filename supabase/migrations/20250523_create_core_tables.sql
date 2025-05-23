-- === Projects ===
create table projects (
  id                bigint primary key,      -- Prefer "projectID" from your CSV as PK
  rcn               text not null unique,
  grant_doi         text,
  title             text,
  start_date        date,
  end_date          date,
  ec_signature      date,
  total_cost        numeric(18, 2),
  ec_contribution   numeric(18, 2),
  created_at        timestamptz default now()
);

-- === Topics (Normalized) ===
create table topics (
  id                text primary key,        -- e.g., 'HORIZON-EIE-2022-CONNECT-01-01'
  title             text
);

create table project_topics (
  project_id        bigint not null references projects(id) on delete cascade,
  topic_id          text not null references topics(id) on delete cascade,
  primary key (project_id, topic_id)
);

-- === Science Vocabulary (Normalized) ===
create table sci_voc (
  code              text primary key,         -- euroSciVocCode
  path              text,
  title             text,
  description       text
);

create table project_sci_voc (
  project_id        bigint not null references projects(id) on delete cascade,
  sci_voc_code      text not null references sci_voc(code) on delete cascade,
  primary key (project_id, sci_voc_code)
);

-- === Organizations ===
create table organizations (
  id                bigint primary key,       -- organisationID
  vat_number        text,
  name              text,
  short_name        text,
  sme               boolean,
  activity_type     text,
  street            text,
  post_code         text,
  city              text,
  country           text,
  nuts_code         text,
  geolocation       text,
  organization_url  text,
  contact_form      text
);

create table project_organizations (
  project_id        bigint not null references projects(id) on delete cascade,
  organization_id   bigint not null references organizations(id) on delete cascade,
  role              text,
  ec_contribution   numeric(18,2),
  net_ec_contribution numeric(18,2),
  total_cost        numeric(18,2),
  end_of_participation boolean,
  active            boolean,
  order_in_project  int,
  rcn               bigint,
  primary key (project_id, organization_id, role)
);

-- === Legal Basis ===
create table legal_basis (
  code              text primary key,         -- legalBasis
  title             text
);

create table project_legal_basis (
  project_id        bigint not null references projects(id) on delete cascade,
  legal_basis_code  text not null references legal_basis(code) on delete cascade,
  unique_programme_part boolean,
  primary key (project_id, legal_basis_code)
);

-- === Deliverables ===
create table deliverables (
  id                text primary key,
  project_id        bigint not null references projects(id) on delete cascade,
  description       text,
  metadata          jsonb not null             -- All extra fields (title, url, etc.)
);

-- === Publications ===
create table publications (
  id                text primary key,
  project_id        bigint not null references projects(id) on delete cascade,
  doi               text,
  metadata          jsonb not null
);

-- === Report Summaries (Optional, Placeholder) ===
create table report_summaries (
  id                text primary key,
  project_id        bigint not null references projects(id) on delete cascade,
  summary           text,
  metadata          jsonb not null
);

-- === Web Items (images, files, etc.) ===
create table web_items (
  id                serial primary key,
  uri               text,
  language          text,
  available_languages text,
  title             text,
  type              text,
  source            text,
  represents        text
);

-- === Web Links (project documents, websites, etc.) ===
create table web_links (
  id                text primary key,
  project_id        bigint references projects(id) on delete cascade,
  phys_url          text,
  available_languages text,
  status            text,
  archived_date     date,
  type              text,
  source            text,
  represents        text
);

-- === Indexes (as before) ===
create index idx_projects_start_date on projects (start_date);
create index idx_projects_end_date on projects (end_date);

create index idx_project_topics_project on project_topics (project_id);
create index idx_project_sci_voc_project on project_sci_voc (project_id);
create index idx_project_organizations_project on project_organizations (project_id);

create index idx_deliverables_project on deliverables (project_id);
create index idx_publications_project on publications (project_id);

create index idx_publications_doi on publications (doi);
create index idx_report_summaries_project on report_summaries (project_id);
create index idx_web_items_uri on web_items (uri);
create index idx_web_links_project on web_links (project_id);
create index idx_web_links_phys_url on web_links (phys_url);
create index idx_web_links_type on web_links (type);

-- You can add more indexes as needed!
