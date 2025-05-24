BEGIN;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 1. PROJECTS
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE public.projects (
  id                         bigint PRIMARY KEY,
  acronym                    text NOT NULL,
  status                     text,
  title                      text,
  start_date                 date,
  end_date                   date,
  total_cost                 numeric,
  ec_max_contribution        numeric,
  ec_signature_date          date,
  framework_programme        text,
  master_call                text,
  sub_call                   text,
  funding_scheme             text,
  nature                     text,
  objective                  text,
  content_update_date        timestamptz,
  rcn                        text UNIQUE,
  grant_doi                  text,
  duration_days              integer,
  duration_months            integer,
  duration_years             integer,
  n_institutions             integer,
  coordinator_name           text,
  ec_contribution_per_year   numeric,
  total_cost_per_year        numeric
);

CREATE INDEX idx_projects_start_date ON public.projects(start_date);
CREATE INDEX idx_projects_end_date   ON public.projects(end_date);

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 2. TOPICS
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE public.topics (
  code   text PRIMARY KEY,
  title  text
);

CREATE TABLE public.project_topics (
  project_id  bigint NOT NULL REFERENCES public.projects(id),
  topic_code  text   NOT NULL REFERENCES public.topics(code),
  PRIMARY KEY (project_id, topic_code)
);

CREATE INDEX idx_project_topics_topic ON public.project_topics(topic_code);

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 3. LEGAL BASIS
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE public.legal_basis (
  code                   text PRIMARY KEY,
  title                  text,
  unique_programme_part  text
);

CREATE TABLE public.project_legal_basis (
  project_id        bigint NOT NULL REFERENCES public.projects(id),
  legal_basis_code  text   NOT NULL REFERENCES public.legal_basis(code),
  PRIMARY KEY (project_id, legal_basis_code)
);

CREATE INDEX idx_project_legal_basis_basis ON public.project_legal_basis(legal_basis_code);

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 4. ORGANIZATIONS
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE public.organizations (
  id                    bigint PRIMARY KEY,
  name                  text NOT NULL,
  short_name            text,
  vat_number            text,
  sme                   boolean,
  activity_type         text,
  street                text,
  post_code             text,
  city                  text,
  country               text,
  nuts_code             text,
  geolocation           text,
  organization_url      text,
  contact_form          text,
  content_update_date   timestamptz
);

CREATE TABLE public.project_organizations (
  project_id           bigint NOT NULL REFERENCES public.projects(id),
  organization_id      bigint NOT NULL REFERENCES public.organizations(id),
  role                 text,
  order_index          integer,
  ec_contribution      numeric,
  net_ec_contribution  numeric,
  total_cost           numeric,
  end_of_participation boolean,
  active               boolean,
  PRIMARY KEY (project_id, organization_id)
);

CREATE INDEX idx_proj_orgs_org ON public.project_organizations(organization_id);

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 5. DELIVERABLES
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE public.deliverables (
  id                    text PRIMARY KEY,
  project_id            bigint NOT NULL REFERENCES public.projects(id),
  title                 text,
  deliverable_type      text,
  description           text,
  url                   text,
  collection            text,
  content_update_date   timestamptz
);

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 6. PUBLICATIONS
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE public.publications (
  id                    text PRIMARY KEY,
  project_id            bigint NOT NULL REFERENCES public.projects(id),
  title                 text,
  is_published_as       text,
  authors               text,
  journal_title         text,
  journal_number        text,
  published_year        integer,
  published_pages       text,
  issn                  text,
  isbn                  text,
  doi                   text,
  collection            text,
  content_update_date   timestamptz
);

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 7. SCIENTIFIC VOCABULARY
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE public.sci_voc (
  code         text PRIMARY KEY,
  path         text,
  title        text,
  description  text
);

CREATE TABLE public.project_sci_voc (
  project_id    bigint NOT NULL REFERENCES public.projects(id),
  sci_voc_code  text   NOT NULL REFERENCES public.sci_voc(code),
  PRIMARY KEY (project_id, sci_voc_code)
);

CREATE INDEX idx_project_sci_voc_sci ON public.project_sci_voc(sci_voc_code);

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 8. WEB ITEMS
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE public.web_items (
  id                   serial PRIMARY KEY,
  language             text,
  available_languages  text[],
  uri                  text,
  title                text,
  type                 text,
  source               text,
  represents           bigint REFERENCES public.projects(id)
);

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- 9. WEB LINKS
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CREATE TABLE public.web_links (
  id                   text PRIMARY KEY,
  project_id           bigint REFERENCES public.projects(id),
  phys_url             text,
  available_languages  text[],
  status               text,
  archived_date        timestamptz,
  type                 text,
  source               text,
  represents           bigint
);

COMMIT;
