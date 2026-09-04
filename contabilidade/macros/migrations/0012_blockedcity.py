from django.db import migrations, models

INITIAL_BLOCKED_CITIES = [
    "Aracaju",
    "Belem",
    "Braganca Paulista",
    "Castanhal",
    "Caxias",
    "Cuiaba",
    "Florianopolis",
    "Joao Pessoa",
    "Jundiai",
    "Londrina",
    "Macaiba",
    "Natal",
    "Ribeirao Preto",
    "Santos",
    "Sao Jose dos Campos",
    "Sao Paulo",
    "Teresina",
    "Uberlandia",
    "Goiania",
]


def seed_blocked_cities(apps, schema_editor):
    BlockedCity = apps.get_model("macros", "BlockedCity")
    from contabilidade.macros.models import normalize_city_name

    for name in INITIAL_BLOCKED_CITIES:
        BlockedCity.objects.get_or_create(
            normalized_name=normalize_city_name(name),
            defaults={"name": name},
        )


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("macros", "0011_macrolead_rtbo_pending_checklist"),
    ]

    operations = [
        migrations.CreateModel(
            name="BlockedCity",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=255, unique=True)),
                ("normalized_name", models.CharField(db_index=True, editable=False, max_length=255, unique=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "ordering": ["name"],
            },
        ),
        migrations.RunPython(seed_blocked_cities, noop_reverse),
    ]
