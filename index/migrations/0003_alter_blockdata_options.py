# Generated by Django 4.0.4 on 2022-05-31 22:24

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('index', '0002_gasfeedata_transactiondata_alter_member_options_and_more'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='blockdata',
            options={'ordering': ['date']},
        ),
    ]
