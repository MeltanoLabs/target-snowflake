from __future__ import annotations

import sys

import click
from sqlalchemy import text

from target_snowflake.connector import SnowflakeConnector


def initializer():
    click.echo("")
    click.echo("")
    click.echo("✨Initializing Snowflake account.✨")
    click.echo("Note: You will always be asked to confirm before anything is executed.")
    click.echo("")
    click.echo(
        "Additionally you can run in `dry_run` mode which will print the SQL without running it.",
    )
    dry_run = click.prompt(
        "Would you like to run in `dry_run` mode?",
        default=False,
        type=bool,
    )
    click.echo("")
    click.echo(
        "We will now interactively create (or the print queries) for all the following objects in your Snowflake account:",  # noqa: E501
    )
    click.echo("    - Role")
    click.echo("    - User")
    click.echo("    - Warehouse")
    click.echo("    - Database")
    click.echo("")
    role = click.prompt("Meltano Role Name:", type=str, default="MELTANO_ROLE")
    user = click.prompt("Meltano User Name:", type=str, default="MELTANO_USER")
    password = click.prompt("Meltano Password", type=str, confirmation_prompt=True)
    warehouse = click.prompt(
        "Meltano Warehouse Name",
        type=str,
        default="MELTANO_WAREHOUSE",
    )
    database = click.prompt(
        "Meltano Database Name",
        type=str,
        default="MELTANO_DATABASE",
    )
    script = SnowflakeConnector.get_initialize_script(
        role,
        user,
        password,
        warehouse,
        database,
    )
    if dry_run:
        click.echo(script)
        sys.exit(0)
    else:
        account = click.prompt("Account (i.e. lqnwlrc-onb17812)", type=str)
        admin_user = click.prompt("User w/SYSADMIN access", type=str)
        admin_pass = click.prompt("User Password", type=str)
        connector = SnowflakeConnector(
            {
                "account": account,
                "database": "SNOWFLAKE",
                "password": admin_pass,
                "role": "SYSADMIN",
                "user": admin_user,
            },
        )

        try:
            click.echo("Initialization Started")
            with connector._connect() as conn:  # noqa: SLF001
                click.echo("Executing:")
                click.echo(f"{script}")
                click.prompt("Confirm?", default=True, type=bool)
                click.echo("Initialization Started...")
                for statement in script.split(";"):
                    if len(statement.strip()) > 0:
                        conn.execute(
                            text(statement),
                        )
                        click.echo("Success!")
            click.echo("Initialization Complete")
        except Exception as e:  # noqa: BLE001
            click.echo(f"Initialization Failed: {e}")
            sys.exit(1)
