-- projects: one row per project
create table projects (
  id              bigint generated always as identity primary key,
  rcn             text not null unique,
  grant_doi       text,
  title           text,
  start_date      date,
  end_date        date,
  ec_signature    date,
  total_cost      numeric,
  ec_contribution numeric,
  topics          jsonb,        -- array of topic IDs
  institutions    jsonb,        -- array of institution IDs
  sci_voc         jsonb,        -- array of vocabulary codes
  created_at      timestamp with time zone default now()
);

-- normalized join table if you need to filter on topics individually:
create table project_topics (
  project_id bigint not null references projects(id),
  topic_id   text     not null,
  primary key (project_id, topic_id)
);

-- similar for deliverables, publications, report_summaries:
create table deliverables (
  id          bigint generated always as identity primary key,
  project_id  bigint not null references projects(id),
  description text,
  metadata    jsonb
);

create table publications (
  id          bigint generated always as identity primary key,
  project_id  bigint not null references projects(id),
  doi         text,
  metadata    jsonb
);

create table report_summaries (
  id          bigint generated always as identity primary key,
  project_id  bigint not null references projects(id),
  summary     text,
  metadata    jsonb
);

create table vis_map{
    id          bigint generated always as identity primary key,
    project_id  bigint not null references projects(id),
    map         jsonb
}

-- Indexes for fast filters
create index on projects (start_date);
create index on projects (end_date);
create gin index on projects using gin (topics);
