from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("macros", "0009_macrolead_signatory_id_macrolead_store_id"),
    ]

    operations = [
        migrations.AddField(
            model_name="macrolead",
            name="export_batch_id",
            field=models.CharField(blank=True, db_index=True, max_length=36),
        ),
        migrations.AddField(
            model_name="macrolead",
            name="exported_at",
            field=models.DateTimeField(blank=True, db_index=True, null=True),
        ),
    ]

