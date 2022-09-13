# target-snowflake

`target-snowflake` is a Singer target for Snowflake.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install target-snowflake
```

## Configuration

### Accepted Config Options

- [ ] `Developer TODO:` Provide a list of config options accepted by the target.

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-snowflake --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

- [ ] `Developer TODO:` If your target requires special access on the source system, or any special authentication requirements, provide those here.

## Usage

You can easily run `target-snowflake` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-snowflake --version
target-snowflake --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-snowflake --config /path/to/target-snowflake-config.json
```

## Developer Resources

- [ ] `Developer TODO:` As a first step, scan the entire project for the text "`TODO:`" and complete any recommended steps, deleting the "TODO" references once completed.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `target_snowflake/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-snowflake` CLI interface directly using `poetry run`:

```bash
poetry run target-snowflake --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-snowflake
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-snowflake --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano elt tap-carbon-intensity target-snowflake
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano SDK to
develop your own Singer taps and targets.
