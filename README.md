# domainradar-training

## Requirements
- Python 3.11
- poetry 1.8 (Please install using pipx)

Note: If you have a newer default Python version, make sure this venv uses 3.11, e.g.:
`poetry env use /usr/local/bin/python3.11`

Install: `poetry install`

Run `poetry shell` brefore running anything here.
Inside the shell, you can run `jupyter notebook` or any other scripts.

Tools for training domain name classifiers that can be integrated into DomainRadar.

## Structure of the repository
- **dga** - Training classifiers for detecting DGA-generated domains
- **phishing** - Training classifier for detecting phishing domains
- **malware** - Training classifier for detecting malware domains
- **feature-extraction** - Tools for loading domain data and extracting feature vectors
