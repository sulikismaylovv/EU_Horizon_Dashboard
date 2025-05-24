import { useEffect, useState } from 'react';
import { supabase } from './supabaseClient';
import { Project } from './interfaces/Projects';
import { Organization } from './interfaces/Organizations';

export default function App() {
//   const [project, setProject] = useState<Project | null>(null);
  const [organization, setOrganization] = useState<Organization | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchRandomProject = async () => {
      setLoading(true);
      const { data, error } = await supabase
        .from('organizations')
        .select('id, name, short_name, street')
        .limit(1)
        .single();
        

      if (error) {
        console.error('Error fetching project:', error);
      } else {
        setOrganization(data);
      }
      setLoading(false);
    };

    fetchRandomProject();
  }, []);

  if (loading) return <p>Loading...</p>;
  if (!organization) return <p>No project found.</p>;

  return (
    <div className="p-4 font-sans">
      <h1 className="text-2xl font-bold mb-2">{organization.id}</h1>
      <h2 className="text-xl mb-2">{organization.name}</h2>
      <p className="mb-2">{organization.short_name}</p>
    </div>
  );
}
