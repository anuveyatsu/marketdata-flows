from datetime import datetime
import urllib.request
import ssl
import json
from dataflows import Flow, load, dump_to_path,dump_to_sql, printer, add_metadata, update_resource
from dataflows import delete_fields, set_type, unpivot, add_computed_field, checkpoint


def get_metadata(resource_id):
    url = 'https://www.energidataservice.dk/api/action/resource_show?id=' + resource_id
    context = ssl._create_unverified_context()
    contents = urllib.request.urlopen(url, context=context).read()
    json_contents = json.loads(contents)
    return json_contents['result']['attributes']


def add_price(rows):
    for row in rows:
        row['PriceDKK'] = row['SpotPriceDKK']
        row['PriceEUR'] = row['SpotPriceEUR']
        yield row


def Elspot_Prices_Data():
    # field_metadata = get_metadata('c86859d2-942e-4029-aec1-32d56f1a2e5d')
    flow = Flow(
        # Load inputs - using 'datastore_search_sql' API load last 10k rows:
        load('https://api.energidataservice.dk/datastore_search_sql?sql=select%20*%20from%20elspotprices%20order%20by%20"HourUTC"%20desc%20limit%20100', format="json", property="result.records", name="fact_elspot_prices"),
        # Remove extra fields:
        delete_fields(fields=['_id', '_full_text','HourDK']),
        # Save the results
        checkpoint('load_data'),
        # Add product:
        add_computed_field([
            dict(target=dict(name='product', type='string'),
                 operation='constant', with_='Elspot'),
            dict(target=dict(name='amount', type='number'),
                 operation='constant', with_=1),
            dict(target=dict(name='PriceDKK', type='number'),
                 operation='constant', with_=-1),
            dict(target=dict(name='PriceEUR', type='number'),
                 operation='constant', with_=-1)
        ]),
        add_price,
        delete_fields(fields=['SpotPriceDKK', 'SpotPriceEUR']),
        add_metadata(name='marketdata', title='Marketdata prototype'),
        update_resource(resources=None, mediatype='text/csv'),
        update_resource(resources='fact_elspot_prices', title='Elspot Prices Data', source='https://www.energidataservice.dk/dataset/elspotprices/resource_extract/c86859d2-942e-4029-aec1-32d56f1a2e5d'),
        printer(),
        dump_to_path('elspot_prices_data'),
        # dump_to_sql(tables={'elspot': {'resource-name': 'Elspot_Prices_Data', 'mode': 'append'}}, engine='postgresql://anuarustayev:vsunokl4@localhost/cubes')
    )
    flow.process()


if __name__ == '__main__':
    Elspot_Prices_Data()
