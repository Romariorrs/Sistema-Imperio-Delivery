from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("macros", "0010_macrolead_export_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="macrolead",
            name="rtbo_pending_checklist",
            field=models.TextField(blank=True),
        ),
    ]
