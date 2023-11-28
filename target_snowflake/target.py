"""Snowflake target class."""

from __future__ import annotations

import click
from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_snowflake.initializer import initializer
from target_snowflake.sinks import SnowflakeSink


class TargetSnowflake(SQLTarget):
    """Target for Snowflake."""

    name = "target-snowflake"
    # From https://docs.snowflake.com/en/user-guide/sqlalchemy.html#connection-parameters
    config_jsonschema = th.PropertiesList(
        th.Property(
            "user",
            th.StringType,
            required=True,
            description="The login name for your Snowflake user.",
        ),
        th.Property(
            "password",
            th.StringType,
            required=False,
            description="The password for your Snowflake user.",
        ),
        th.Property(
            "private_key_path",
            th.StringType,
            required=False,
            description="Path to file containing private key.",
        ),
        th.Property(
            "private_key_passphrase",
            th.StringType,
            required=False,
            description="Passphrase to decrypt private key if encrypted.",
        ),
        th.Property(
            "account",
            th.StringType,
            required=True,
            description="Your account identifier. See [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html).",
        ),
        th.Property(
            "database",
            th.StringType,
            required=True,
            description="The initial database for the Snowflake session.",
        ),
        th.Property(
            "schema",
            th.StringType,
            description="The initial schema for the Snowflake session.",
        ),
        th.Property(
            "warehouse",
            th.StringType,
            description="The initial warehouse for the session.",
        ),
        th.Property(
            "role",
            th.StringType,
            description="The initial role for the session.",
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType,
            default=True,
            description="Whether to add metadata columns.",
        ),
        th.Property(
            "clean_up_batch_files",
            th.BooleanType,
            default=True,
            description="Whether to remove batch files after processing.",
        ),
    ).to_dict()

    default_sink_class = SnowflakeSink

    @classmethod
    def cb_inititalize(
        cls: type[TargetSnowflake],
        ctx: click.Context,
        param: click.Option,  # noqa: ARG003
        value: bool,  # noqa: FBT001
    ) -> None:
        if value:
            initializer()
            ctx.exit()

    @classmethod
    def get_singer_command(cls: type[TargetSnowflake]) -> click.Command:
        """Execute standard CLI handler for targets.

        Returns:
            A click.Command object.
        """
        command = super().get_singer_command()
        command.params.extend(
            [
                click.Option(
                    ["--initialize"],
                    is_flag=True,
                    help="Interactive Snowflake account initialization.",
                    callback=cls.cb_inititalize,
                    expose_value=False,
                ),
            ],
        )

        return command


if __name__ == "__main__":
    TargetSnowflake.cli()
