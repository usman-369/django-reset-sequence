from psycopg2 import sql
from django.apps import apps
from django.db import connection
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Reset sequence of a model's primary key. Optionally delete one or more rows by ID."

    def add_arguments(self, parser):
        parser.add_argument("app_label", type=str, help="App label, e.g., 'funds'")
        parser.add_argument(
            "model_name", type=str, help="Model name, e.g., 'FundCategoryType'"
        )
        parser.add_argument(
            "--delete-ids",
            nargs="+",
            type=int,
            help="IDs of the rows to delete (space-separated).",
        )
        parser.add_argument(
            "--delete-only",
            action="store_true",
            help="Only delete rows, do not reset the sequence.",
        )

    def handle(self, *args, **options):
        app_label = options["app_label"]
        model_name = options["model_name"]
        delete_ids = options.get("delete_ids")
        delete_only = options.get("delete_only")

        try:
            model = apps.get_model(app_label, model_name)
        except LookupError:
            raise CommandError(f"Model {app_label}.{model_name} not found.")

        table_name = model._meta.db_table
        actions = []

        # Delete row(s) if provided
        if delete_ids:
            deleted_count, _ = model.objects.filter(pk__in=delete_ids).delete()
            if deleted_count > 0:
                msg = f"Deleted {deleted_count} row(s) with id(s) {delete_ids} from {table_name}."
                self.stdout.write(self.style.WARNING(msg))
                actions.append(msg)
            else:
                msg = f"No row(s) with id(s) {delete_ids} found in {table_name}."
                self.stdout.write(self.style.ERROR(msg))
                actions.append(msg)

        # Reset sequence unless delete-only mode
        if not delete_only:
            query = sql.SQL(
                """
                SELECT setval(
                    pg_get_serial_sequence({table_literal}, 'id'),
                    COALESCE((SELECT MAX(id) FROM {table_ident}), 1) + 1,
                    false
                )
                """
            ).format(
                table_literal=sql.Literal(table_name),
                table_ident=sql.Identifier(table_name),
            )

            with connection.cursor() as cursor:
                cursor.execute(query)  # type: ignore[arg-type]

            msg = f"Sequence reset for {table_name} successfully."
            self.stdout.write(self.style.SUCCESS(msg))
            actions.append(msg)

        if not actions:
            self.stdout.write(self.style.NOTICE("No action performed."))
