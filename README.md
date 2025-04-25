# MSc in Data Engineering & AI (Self-Designed)
A 12-month program focused on building real-world expertise in data engineering and machine learning using GCP, Python, and modern tooling.

## Local Set-Up
The local environment uses a VS Code dev container setup.

### Prerequisites
- VS Code is the code editor required to spin up the local devcontainer
- Docker is required for containerisation
- Google Cloud SDK should be set up with default credentials created at `~/.config/gcloud/application_default_credentials.json` by following these steps:
    - Install the gcloud CLI:
        - MacOs: run `brew install --cask google-cloud-sdk`
        - Windows: Download from here: https://cloud.google.com/sdk/docs/install
    - Run `gcloud init` in your command line. This will:
        - Log you into your Google account
        - Set a default project
        - Configure your CLI environment
    - Authenticate with Application Default Credentials by running: `gcloud auth application-default login`

### Spinning up local dev environment

There are two ways you can spin up the Docker VS Code dev container, both rely on the Docker daemon running. Open the repository in VS Code and you should be prompted to `Reopen in Container`:
![Reopen in Container](images/reopen_in_container.png)

If the prompt disappears, simply press Command+Shft+P on a Mac or Ctrl+Shft+P on Windows and search Rebuild and Reopen in Container:

![Rebuild and Reopen in Container](images/rebuild_and_reopen_in_container.png)
