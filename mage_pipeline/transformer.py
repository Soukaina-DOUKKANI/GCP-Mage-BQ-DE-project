if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd

@transformer
def transform(file, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    file['tpep_pickup_datetime']=pd.to_datetime(file['tpep_pickup_datetime'])
    file['tpep_dropoff_datetime']=pd.to_datetime(file['tpep_dropoff_datetime'])
    file["trip_id"]=file.index
    datetime_dim=file[['tpep_pickup_datetime','tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
    datetime_dim['pick_hour']=datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day']=datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month']=datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year']=datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday']=datetime_dim['tpep_pickup_datetime'].dt.weekday
    #all information related to dropout datetime 
    datetime_dim['drop_hour']=datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day']=datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month']=datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year']=datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday']=datetime_dim['tpep_dropoff_datetime'].dt.weekday
    #add primary key 
    datetime_dim['datetime_id']=datetime_dim.index
    #passenger_count_dim 
    passenger_count_dim=file[['passenger_count']]
    passenger_count_dim['passenger_count_id']=passenger_count_dim.index
    passenger_count_dim=passenger_count_dim[['passenger_count_id','passenger_count']]
    #trip distance dim 
    trip_distance_dim=file[['trip_distance']].drop_duplicates().reset_index(drop=True)
    trip_distance_dim['trip_distance_id']=trip_distance_dim.index
    trip_distance_dim=trip_distance_dim[['trip_distance_id', 'trip_distance']]
    #rate code dim 
    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    rate_code_dim = file[['RatecodeID']].reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]
    #payment type dim 
    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }
    payment_type_dim = file[['payment_type']].reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]
    # pickup and dropoff dims 
    pickup_location_dim = file[['pickup_longitude', 'pickup_latitude']].reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_latitude','pickup_longitude']] 

    #dropoff dim
    dropoff_location_dim = file[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]
    #fact_table
    fact_table = file.merge(passenger_count_dim, left_on='trip_id', right_on='passenger_count_id') \
        .merge(trip_distance_dim, left_on='trip_id', right_on='trip_distance_id') \
        .merge(rate_code_dim, left_on='trip_id', right_on='rate_code_id') \
        .merge(pickup_location_dim, left_on='trip_id', right_on='pickup_location_id') \
        .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id')\
        .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
        .merge(payment_type_dim, left_on='trip_id', right_on='payment_type_id') \
        [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
        'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
        'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'total_amount']]
    
    print('all done!')
    return {"datetime_dim":datetime_dim.to_dict(orient="dict"),
        "passenger_count_dim":passenger_count_dim.to_dict(orient="dict"),
        "trip_distance_dim":trip_distance_dim.to_dict(orient="dict"),
        "rate_code_dim":rate_code_dim.to_dict(orient="dict"),
        "pickup_location_dim":pickup_location_dim.to_dict(orient="dict"),
        "dropoff_location_dim":dropoff_location_dim.to_dict(orient="dict"),
        "payment_type_dim":payment_type_dim.to_dict(orient="dict"),
        "fact_table":fact_table.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
