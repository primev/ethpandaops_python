# ethpandaops-python

### Getting Started

1. This repository uses rye to manage dependencies and the virtual environment. To install, refer to this link for instructions [here](https://rye-up.com/guide/installation/). 
2. Once rye is installed, run `rye sync` to install dependencies and setup the virtual environment, which has a default name of `.venv`. 
3. Activate the virtual environment with the command `source .venv/bin/activate`.
4. Once everything is installed, add, create a `.env` file and add the following local variables. Clickhouse will use these credentials for authentication to run the qeries. 

### local variables
To run the clickhouse queries, add the following variables to the .env file

```
USERNAME=xxx_username_xxx
PASSWORD=xxx_password_xxx
HOST="clickhouse.analytics.production.platform.ethpandaops.io"
```

### Example
In the examples folder, the `canonical_beacon_chain.py` file has the most up to date logic and comments. This file is a good starting point to understand how to see the overall library flow.