from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("macros", "0006_macrolead_business_99_status"),
    ]

    operations = [
        migrations.AddField(
            model_name="macrorun",
            name="pages_processed",
            field=models.PositiveIntegerField(default=0),
        ),
    ]
