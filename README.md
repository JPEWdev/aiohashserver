# Asynchronous Hash Equivalence Server

Implements a fully asynchronous hash equivalence server for bitbake using
[aiohttp](https://github.com/aio-libs/aiohttp/).

## Running

aiohashserver uses [pipenv](https://github.com/pypa/pipenv) to manage
dependencies. Please refer to the documentation to install it.

Once pipenv is installed, aiohashserver can be started by running:

```shell
pipenv update && pipenv run ./hashserver.py
```

