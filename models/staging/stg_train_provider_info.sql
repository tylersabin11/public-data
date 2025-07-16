select 
    Provider as provider,
    PotentialFraud as potential_fraud
from {{ source('my_datasets', 'train_1542865627584' )}}