from datetime import datetime
from dataflows import Flow, load, dump_to_path, concatenate, add_computed_field
from dataflows import set_type, set_primary_key, duplicate, select_fields, join_self


def add_id(rows):
    counter = 0
    for row in rows:
        row['id'] = counter + 1
        yield row
        counter += 1


def add_foreign_keys(package):
    package.pkg.descriptor['resources'][0]['schema']['foreignKeys'] = [
        {
            'fields': 'Timestamp',
            'reference': {
                'resource': 'time',
                'fields': 'Timestamp'
            }
        },
        {
            'fields': 'Area',
            'reference': {
                'resource': 'area',
                'fields': 'Area'
            }
        },
        {
            'fields': 'Product',
            'reference': {
                'resource': 'product',
                'fields': 'Product'
            }
        }
    ]
    # Must yield the modified datapackage
    yield package.pkg
    # And its resources
    yield from package


def Olap_Datapackage():
    flow = Flow(
        # Load datapackages:
        load('elspot_prices_data/datapackage.json'),
        load('afrr_data/datapackage.json'),
        load('fcr_dk1_data/datapackage.json'),
        concatenate(
            fields={
                'Timestamp': ['HourUTC'], 'Area': ['PriceArea'],
                'Product': ['product'], 'Amount': ['amount'],
                'Price_DKK': ['PriceDKK'], 'Price_EUR': ['PriceEUR']
            },
            target={'name':'fact', 'path':'data/fact.csv'}
        ),
        add_computed_field([dict(target='id', operation='constant', with_='dummy')]),
        add_id,
        set_type('id', type='integer'),
        set_primary_key(primary_key=['id']),
        # Reorder so that 'id' column is the first:
        select_fields(['id', 'Timestamp', 'Area', 'Product', 'Amount', 'Price_DKK', 'Price_EUR'], resources='fact'),
        # Add foreign keys:
        add_foreign_keys,
        # Fact table is ready. Now duplicate the resource to generate dim tables:
        # First is 'time' table:
        duplicate(source='fact', target_name='time', target_path='time.csv'),
        select_fields(['Timestamp'], resources=['time']),
        join_self(source_name='time', source_key=['Timestamp'], target_name='time', fields={'Timestamp': {}}),
        # Parse datetime fields and add a separate field for year, month and day:
        add_computed_field([
            dict(target=dict(name='day', type='string'),
                 operation=lambda row: datetime.strptime(row['Timestamp'], '%Y-%m-%dT%H:%M:%S+00:00').strftime('%d')),
            dict(target=dict(name='month', type='string'),
                 operation=lambda row: datetime.strptime(row['Timestamp'], '%Y-%m-%dT%H:%M:%S+00:00').strftime('%m')),
            dict(target=dict(name='month_name', type='string'),
                 operation=lambda row: datetime.strptime(row['Timestamp'], '%Y-%m-%dT%H:%M:%S+00:00').strftime('%B')),
            dict(target=dict(name='year', type='year'),
                 operation=lambda row: datetime.strptime(row['Timestamp'], '%Y-%m-%dT%H:%M:%S+00:00').strftime('%Y')),
        ], resources=['time']),
        set_primary_key(primary_key=['Timestamp'], resources=['time']),
        # Now 'area' table:
        duplicate(source='fact', target_name='area', target_path='area.csv'),
        select_fields(['Area'], resources=['area']),
        join_self(source_name='area', source_key=['Area'], target_name='area', fields={'Area': {}}),
        set_primary_key(primary_key=['Area'], resources=['area']),
        # Now 'product' table:
        duplicate(source='fact', target_name='product', target_path='product.csv'),
        select_fields(['Product'], resources=['product']),
        join_self(source_name='product', source_key=['Product'], target_name='product', fields={'Product': {}}),
        set_primary_key(primary_key=['Product'], resources=['product']),
        dump_to_path('olap_datapackage')
    )
    flow.process()


if __name__ == '__main__':
    Olap_Datapackage()
