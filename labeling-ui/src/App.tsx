import { useCallback, useEffect, useState } from 'react'
import { fetchNextRepo, submitLabel, type Label, type Repo } from './api'

const LABELER_STORAGE_KEY = 'peep_labeler'

function formatDate(iso: string | null): string {
  if (!iso) return '—'
  return new Date(iso).toLocaleDateString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  })
}

function formatNumber(n: number | null): string {
  if (n === null || n === undefined) return '—'
  return n.toLocaleString()
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-gray-200 bg-white p-3">
      <div className="text-xs font-medium text-gray-500">{label}</div>
      <div className="mt-1 text-sm font-semibold text-gray-900">{value}</div>
    </div>
  )
}

function LabelerGate({ onSet }: { onSet: (name: string) => void }) {
  const [name, setName] = useState('')

  return (
    <div className="flex min-h-screen items-center justify-center bg-gray-50">
      <form
        className="w-80 rounded-xl border border-gray-200 bg-white p-6 shadow-sm"
        onSubmit={(e) => {
          e.preventDefault()
          const trimmed = name.trim()
          if (trimmed) onSet(trimmed)
        }}
      >
        <h1 className="text-lg font-semibold text-gray-900">Peep Labeling</h1>
        <p className="mt-1 text-sm text-gray-500">
          Who's labeling? This tags every label you submit.
        </p>
        <input
          autoFocus
          className="mt-4 w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-gray-500 focus:outline-none"
          placeholder="your name"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <button
          type="submit"
          disabled={!name.trim()}
          className="mt-3 w-full rounded-md bg-gray-900 px-3 py-2 text-sm font-medium text-white disabled:opacity-40"
        >
          Start labeling
        </button>
      </form>
    </div>
  )
}

function App() {
  const [labeler, setLabeler] = useState<string | null>(() =>
    localStorage.getItem(LABELER_STORAGE_KEY),
  )
  const [repo, setRepo] = useState<Repo | null>(null)
  const [loading, setLoading] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const loadNext = useCallback(async () => {
    if (!labeler) return
    setLoading(true)
    setError(null)
    try {
      const next = await fetchNextRepo(labeler)
      setRepo(next)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load next repo')
    } finally {
      setLoading(false)
    }
  }, [labeler])

  useEffect(() => {
    loadNext()
  }, [loadNext])

  const handleSetLabeler = (name: string) => {
    localStorage.setItem(LABELER_STORAGE_KEY, name)
    setLabeler(name)
  }

  const handleLabel = async (label: Label) => {
    if (!repo || !labeler) return
    setSubmitting(true)
    setError(null)
    try {
      await submitLabel(repo.repo_id, label, labeler)
      await loadNext()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to submit label')
    } finally {
      setSubmitting(false)
    }
  }

  if (!labeler) return <LabelerGate onSet={handleSetLabeler} />

  return (
    <div className="min-h-screen bg-gray-50 px-4 py-10">
      <div className="mx-auto max-w-2xl">
        <div className="mb-6 flex items-center justify-between">
          <h1 className="text-lg font-semibold text-gray-900">Peep Labeling</h1>
          <span className="text-sm text-gray-500">labeling as {labeler}</span>
        </div>

        {error && (
          <div className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
            {error}
          </div>
        )}

        {loading && (
          <div className="rounded-xl border border-gray-200 bg-white p-8 text-center text-sm text-gray-500">
            Loading…
          </div>
        )}

        {!loading && !repo && !error && (
          <div className="rounded-xl border border-gray-200 bg-white p-8 text-center text-sm text-gray-500">
            Nothing left to label right now — check back later.
          </div>
        )}

        {!loading && repo && (
          <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
            <a
              href={`https://github.com/${repo.owner_login}/${repo.repo_name}`}
              target="_blank"
              rel="noreferrer"
              className="text-xl font-semibold text-blue-600 hover:underline"
            >
              {repo.owner_login}/{repo.repo_name}
            </a>
            <p className="mt-1 text-sm text-gray-500">
              repo created {formatDate(repo.created_at)} · account created{' '}
              {formatDate(repo.account_created_at)}
            </p>

            <div className="mt-5 grid grid-cols-2 gap-3 sm:grid-cols-3">
              <Stat label="Stars" value={formatNumber(repo.stars)} />
              <Stat label="Forks" value={formatNumber(repo.forks)} />
              <Stat label="Watchers" value={formatNumber(repo.watchers)} />
              <Stat label="Open issues" value={formatNumber(repo.open_issues_count)} />
              <Stat label="Commits" value={formatNumber(repo.commit_count_approx)} />
              <Stat
                label="Commits/day"
                value={repo.commits_per_day ? repo.commits_per_day.toFixed(1) : '—'}
              />
              <Stat label="Contributors" value={formatNumber(repo.contributor_count_approx)} />
              <Stat label="Files" value={formatNumber(repo.file_count)} />
              <Stat
                label="Top extension"
                value={
                  repo.top_extension
                    ? `.${repo.top_extension} (${
                        repo.extension_homogeneity_ratio !== null
                          ? Math.round(repo.extension_homogeneity_ratio * 100)
                          : '?'
                      }%)`
                    : '—'
                }
              />
              <Stat label="Language" value={repo.primary_language ?? '—'} />
              <Stat label="Size" value={repo.size_kb !== null ? `${formatNumber(repo.size_kb)} KB` : '—'} />
              <Stat
                label="Description / license"
                value={`${repo.description_present ? 'yes' : 'no'} / ${repo.license_present ? 'yes' : 'no'}`}
              />
            </div>

            {(repo.is_fork || repo.archived) && (
              <div className="mt-4 flex gap-2">
                {repo.is_fork && (
                  <span className="rounded-full bg-yellow-100 px-2 py-0.5 text-xs font-medium text-yellow-800">
                    fork
                  </span>
                )}
                {repo.archived && (
                  <span className="rounded-full bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-600">
                    archived
                  </span>
                )}
              </div>
            )}

            <div className="mt-6 flex gap-3">
              <button
                type="button"
                disabled={submitting}
                onClick={() => handleLabel('legitimate')}
                className="flex-1 rounded-md bg-green-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-green-700 disabled:opacity-40"
              >
                Legitimate
              </button>
              <button
                type="button"
                disabled={submitting}
                onClick={() => handleLabel('suspicious')}
                className="flex-1 rounded-md bg-red-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-red-700 disabled:opacity-40"
              >
                Suspicious
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default App
