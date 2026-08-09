const API_BASE_URL = import.meta.env.VITE_API_BASE_URL as string

// Only the fields the labeling UI actually displays -- the backend's
// SELECT * returns the full repos row (see SCHEMA.md), this is a subset.
export interface Repo {
  repo_id: number
  owner_login: string
  repo_name: string
  created_at: string | null
  account_created_at: string | null
  description_present: boolean | null
  description_length: number | null
  primary_language: string | null
  license_present: boolean | null
  size_kb: number | null
  stars: number | null
  forks: number | null
  watchers: number | null
  open_issues_count: number | null
  commit_count_approx: number | null
  commits_per_day: number | null
  contributor_count_approx: number | null
  file_count: number | null
  top_extension: string | null
  extension_homogeneity_ratio: number | null
  is_fork: boolean | null
  archived: boolean | null
}

export type Label = 'legitimate' | 'suspicious'

export async function fetchNextRepo(labeler: string): Promise<Repo | null> {
  const res = await fetch(
    `${API_BASE_URL}/repos/next?labeler=${encodeURIComponent(labeler)}`,
  )
  if (res.status === 204) return null
  if (!res.ok) throw new Error(`GET /repos/next failed: ${res.status}`)
  return res.json()
}

export async function submitLabel(
  repoId: number,
  label: Label,
  labeledBy: string,
): Promise<void> {
  const res = await fetch(`${API_BASE_URL}/repos/${repoId}/label`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ label, labeled_by: labeledBy }),
  })
  if (!res.ok) throw new Error(`POST /repos/${repoId}/label failed: ${res.status}`)
}
