from tools.migrate_public_worker_workflows import transform_workflow


def test_transform_workflow_makes_private_checkout_a_public_self_checkout() -> None:
    source = """
on:
  workflow_dispatch:
    inputs:
      yahoo_oauth_ref:
        description: private source ref
        default: main
jobs:
  worker:
    steps:
      - uses: actions/checkout@v5
        with:
          repository: jeleff1000/yahoo_oauth
          token: ${{ secrets.PRIVATE_REPO_PAT }}
          ref: ${{ inputs.yahoo_oauth_ref || 'main' }}
          filter: blob:none
          sparse-checkout: |
            /scripts/
      - run: echo ${{ inputs.yahoo_oauth_ref }}
""".lstrip()

    transformed = transform_workflow(source)

    assert "repository: jeleff1000/yahoo_oauth" not in transformed
    assert "PRIVATE_REPO_PAT" not in transformed
    assert "yahoo_oauth_ref" not in transformed
    assert "worker_ref" not in transformed
    assert "echo main" in transformed
    assert "filter: blob:none" in transformed
    assert "/scripts/" in transformed


def test_transform_workflow_routes_repository_calls_and_tokens_to_public_repo() -> None:
    source = """
env:
  GH_TOKEN: ${{ secrets.PRIVATE_REPO_PAT || github.token }}
steps:
  - run: gh api repos/league-history-workers/mfl-league-fetcher/actions/runs
  - run: gh api repos/jeleff1000/yahoo_oauth/actions/runs
  - run: gh workflow run job.yml -R league-history-workers/league-history-workers
""".lstrip()

    transformed = transform_workflow(source)

    assert "GH_TOKEN: ${{ github.token }}" in transformed
    assert transformed.count("jeleff1000/league-history-workers") == 3
    assert "mfl-league-fetcher" not in transformed
    assert "league-history-workers/league-history-workers" not in transformed
