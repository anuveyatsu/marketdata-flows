from dataflows import Flow, load, dump_to_path,dump_to_sql, printer, add_metadata, update_resource
from dataflows import delete_fields, set_type, unpivot, add_computed_field, checkpoint


def add_price(rows):
    for row in rows:
        if row['product'] == 'aFRR_DownActivated':
            row['PriceDKK'] = row['aFRR_DownPriceDKK']
            row['PriceEUR'] = row['aFRR_DownPriceEUR']
        elif row['product'] == 'aFRR_UpActivated':
            row['PriceDKK'] = row['aFRR_UpPriceDKK']
            row['PriceEUR'] = row['aFRR_UpPriceEUR']
        yield row

def AFRR_Data():
    unpivoting_fields = [
        {'name': 'aFRR_DownActivated', 'keys': {'product': 'aFRR_DownActivated'}},
        {'name': 'aFRR_UpActivated', 'keys': {'product': 'aFRR_UpActivated'}}
    ]
    extra_keys = [
        {'name': 'product', 'type': 'string'}
    ]
    extra_value = {'name': 'amount', 'type': 'number'}
    flow = Flow(
        # Load inputs - using 'datastore_search_sql' API load last 10k rows:
        load('https://api.energidataservice.dk/datastore_search_sql?sql=select%20*%20from%20afrrreservesdk1%20order%20by%20"HourUTC"%20desc%20limit%201000', format="json", property="result.records", name="fact_afrr"),
        # Remove extra fields:
        delete_fields(fields=['_id', '_full_text','HourDK']),
        # Save the results
        checkpoint('afrr'),
        # Normalize/unpivot:
        unpivot(unpivoting_fields, extra_keys, extra_value),
        add_computed_field([
            dict(target=dict(name='PriceArea', type='string'),
                 operation='constant', with_='DK1'),
            dict(target=dict(name='PriceDKK', type='number'),
                 operation='constant', with_='dummy'),
            dict(target=dict(name='PriceEUR', type='number'),
                 operation='constant', with_='dummy')
        ]),
        add_price,
        delete_fields(fields=['aFRR_DownPriceDKK', 'aFRR_DownPriceEUR', 'aFRR_UpPriceDKK', 'aFRR_UpPriceEUR']),
        add_metadata(name='marketdata', title='Marketdata prototype'),
        update_resource(resources=None, mediatype='text/csv'),
        update_resource(resources='fact_afrr', title='Automatic Frequency Restoration Reserves', source='https://www.energidataservice.dk/dataset/afrrreservesdk1/resource_extract/0694e216-6713-4f84-9b98-7bb5bc11d80c'),
        printer(),
        dump_to_path('afrr_data')
    )
    flow.process()


if __name__ == '__main__':
    AFRR_Data()
