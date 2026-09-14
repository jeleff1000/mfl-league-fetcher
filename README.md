# League History Workers

GitHub Actions workflows for [League History](https://leaguehistory.app) data import and processing.

This is the public, self-contained execution repository. Worker workflows use
the code pinned by their own run SHA and do not check out a private source repo.

## Setup

Configure only the platform and Fly credentials required by the workflows you
run, such as `YAHOO_CLIENT_ID`, `YAHOO_CLIENT_SECRET`,
`CREDENTIAL_ENCRYPTION_KEY`, and the `DATABASE_*` secrets. A private-repository
token is neither required nor supported.
