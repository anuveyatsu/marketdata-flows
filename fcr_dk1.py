from dataflows import Flow, load, dump_to_path,dump_to_sql, printer, add_metadata, update_resource
from dataflows import delete_fields, set_type, unpivot, add_computed_field, checkpoint


def add_values(rows):
    for row in rows:
        if row['product'] == 'FCR_Down_Expected' or row['product'] == 'FCR_Up_Expected':
            row['PriceDKK'] = ''
            row['PriceEUR'] = ''
        elif row['product'] == 'FCR_Down_Purchased':
            row['PriceDKK'] = row['FCR_DownPriceDKK']
            row['PriceEUR'] = row['FCR_DownPriceEUR']
        elif row['product'] == 'FCR_Up_Purchased':
            row['PriceDKK'] = row['FCR_UpPriceDKK']
            row['PriceEUR'] = row['FCR_UpPriceEUR']
        yield row


def FCR_DK1_Data():
    unpivoting_fields = [
        {'name': 'FCR_DownExpected', 'keys': {'product': 'FCR_Down_Expected'}},
        {'name': 'FCR_UpExpected', 'keys': {'product': 'FCR_Up_Expected'}},
        {'name': 'FCR_DownPurchased', 'keys': {'product': 'FCR_Down_Purchased'}},
        {'name': 'FCR_UpPurchased', 'keys': {'product': 'FCR_Up_Purchased'}}
    ]
    extra_keys = [
        {'name': 'product', 'type': 'string'}
    ]
    extra_value = {'name': 'amount', 'type': 'number'}
    flow = Flow(
        # Load inputs - using 'datastore_search_sql' API load last 10k rows:
        load('https://api.energidataservice.dk/datastore_search_sql?sql=select%20*%20from%20fcrreservesdk1%20order%20by%20"HourUTC"%20desc%20limit%201000', format="json", property="result.records", name="fact_fcr_dk1"),
        # Remove extra fields:
        delete_fields(fields=['_id', '_full_text','HourDK']),
        # Save the results
        checkpoint('fcr_dk1_data'),
        # Normalize/unpivot:
        unpivot(unpivoting_fields, extra_keys, extra_value),
        add_computed_field([
            dict(target=dict(name='PriceArea', type='string'),
                 operation='constant', with_='DK1'),
            dict(target=dict(name='PriceDKK', type='number'),
                 operation='constant', with_=-1),
            dict(target=dict(name='PriceEUR', type='number'),
                 operation='constant', with_=-1)
        ]),
        add_values,
        delete_fields(fields=['FCR_DownPriceDKK', 'FCR_DownPriceEUR', 'FCR_UpPriceDKK', 'FCR_UpPriceEUR',]),
        add_metadata(name='marketdata', title='Marketdata prototype'),
        update_resource(resources=None, mediatype='text/csv'),
        update_resource(resources='fact_fcr_dk1', title='Frequency Containment Reserves, DK1 Data', source='https://www.energidataservice.dk/dataset/fcrreservesdk1/resource_extract/45433ada-5648-4d83-8eab-9d642db18ca5'),
        printer(),
        dump_to_path('fcr_dk1_data')
    )
    flow.process()


if __name__ == '__main__':
    FCR_DK1_Data()
