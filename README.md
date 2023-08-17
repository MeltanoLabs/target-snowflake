# `target-snowflake`

Target for Snowflake.

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Capabilities

* `about`
* `stream-maps`
* `schema-flattening`
* `target-schema`

## Settings

| Setting                | Required | Default | Description                                                                                                                                 |
|:-----------------------|:--------:|:-------:|:--------------------------------------------------------------------------------------------------------------------------------------------|
| user                   | True     | None    | The login name for your Snowflake user.                                                                                                     |
| password               | False    | None    | The password for your Snowflake user.                                                                                                       |
| private_key_path       | False    | None    | Path to private key file. See [Key pair auth](https://docs.snowflake.com/en/user-guide/key-pair-auth)                                       |
| private_key_passphrase | False    | None    | Passphrase to decrypt private key if encrypted.                                                                                             |
| account                | True     | None    | Your account identifier. See [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html).                 |
| database               | True     | None    | The initial database for the Snowflake session.                                                                                             |
| schema                 | False    | None    | The initial schema for the Snowflake session.                                                                                               |
| warehouse              | False    | None    | The initial warehouse for the session.                                                                                                      |
| role                   | False    | None    | The initial role for the session.                                                                                                           |
| add_record_metadata    | False    |       1 | Whether to add metadata columns.                                                                                                            |
| clean_up_batch_files   | False    |       1 | Whether to remove batch files after processing.                                                                                             |
| default_target_schema  | False    | None    | The default target database schema name to use for all streams.                                                                             |
| stream_maps            | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config      | False    | None    | User-defined config values to be used within map expressions.                                                                               |
| flattening_enabled     | False    | None    | 'True' to enable schema flattening and automatically expand nested properties.                                                              |
| flattening_max_depth   | False    | None    | The max depth to flatten schemas.                                                                                                           |

A full list of supported settings and capabilities is available by running: `target-snowflake --about`

### Initializing a Snowflake Account

This target has an interactive feature that will help you get a Snowflake account initialized with everything needed to get started loading data.

- User
- Role
- Warehouse
- Database
- Proper grants

The CLI will ask you to provide information about the new user/role/etc. you want to create but it will also need SYSADMIN credentials to execute the queries.
You should prepare the following inputs:

- Account
- User that has SYSADMIN and SECURITYADMIN access. These comes default with the user that created the Snowflake account.
- The password for your SYSADMIN user.

Run the following command to get started with the interactive CLI.
Note - the CLI will print the SQL queries it is planning to run and confirm with you before it makes any changes.

```bash
poetry run target-snowflake --initialize

# Alternatively using Meltano CLI
meltano invoke target-snowflake --initialize
```

The CLI also has a "dry run" mode that will print the queries without executing them.

Check out the demo of this [on YouTube](https://youtu.be/9vEFxw-0nxI).

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

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

Your project comes with a custom `meltano.yml` project file already created.

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
meltano run tap-carbon-intensity target-snowflake
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano SDK to
develop your own Singer taps and targets.
