from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("macros", "0007_macrorun_pages_processed"),
    ]

    operations = [
        migrations.AddField(
            model_name="macrorun",
            name="execution_id",
            field=models.CharField(blank=True, db_index=True, max_length=64),
        ),
        migrations.AddField(
            model_name="macrorun",
            name="total_deduplicated",
            field=models.PositiveIntegerField(default=0),
        ),
    ]
