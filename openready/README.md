## How to use serdif-api to query data

* Include the query functions from api_data.py

```python
from openready.api_openready import serdifDataAPI
```
* Include a python dictionary following the structure below:
```python
exampleDict = {
    # Event name requires the 'http://example.org/ns#' prefix
    'http://example.org/ns#event-test-01': {
        'event': 'http://example.org/ns#event-test-01',
        # Date of the event
        'evDateT': '2017-09-10T00:00:00Z',
        # Select region for the environmental data
        'region': ['WEXFORD'],
        # Define the time-window for the environmental data
        'wLen': 3,  # Duration in days (length)
        'wLag': 4,  # Lag from the event date in days
    },
    'http://example.org/ns#event-test-02': {
        'event': 'http://example.org/ns#event-test-02',
        # Date of the event
        'evDateT': '2015-01-10T00:00:00Z',
        # Select region for the environmental data
        'region': ['DUBLIN'],
        # Define the time-window for the environmental data
        'wLen': 10,  # Duration in days (length)
        'wLag': 0,   # Lag from the event date in days
    }
    # You can include as many events as you need
}
```
* Use the serdifAPI function imported above with the following options
```python
exampleData = serdifAPI(
    evEnvoDict=exampleDict,
    # Select temporal units for the datasets used with environmental
    # data from: 'hour', 'day', 'month' or 'year'
    timeUnit='hour',
    # Select spatiotemporal aggregation method for the datasets
    # used with environmental data from: 'AVG', 'SUM', 'MIN' or 'MAX'
    spAgg='AVG',
    # Login credentials for https://serdif-example.adaptcentre.ie/
    username='',
    password='',
    # Select the returning data format as 'CSV' or 'RDF'
    dataFormat='CSV'
)
```
* Full example:
```python
from api_data import serdifAPI
# Define your events
exampleDict = {
    # Event name requires the 'http://example.org/ns#' prefix
    'http://example.org/ns#event-test-01': {
        'event': 'http://example.org/ns#event-test-01',
        # Date of the event
        'evDateT': '2017-09-10T00:00:00Z',
        # Select region for the environmental data
        'region': ['WEXFORD'],
        # Define the time-window for the environmental data
        'wLen': 3,  # Duration in days (length)
        'wLag': 4,  # Lag from the event date in days
    },
    'http://example.org/ns#event-test-02': {
        'event': 'http://example.org/ns#event-test-02',
        # Date of the event
        'evDateT': '2015-01-10T00:00:00Z',
        # Select region for the environmental data
        'region': ['DUBLIN'],
        # Define the time-window for the environmental data
        'wLen': 10,  # Duration in days (length)
        'wLag': 0,   # Lag from the event date in days
    }
    # You can include as many events as you need
}
# Query the data
exampleData = serdifAPI(
    evEnvoDict=exampleDict,
    # Select temporal units for the datasets used with environmental
    # data from: 'hour', 'day', 'month' or 'year'
    timeUnit='hour',
    # Select spatiotemporal aggregation method for the datasets
    # used with environmental data from: 'AVG', 'SUM', 'MIN' or 'MAX'
    spAgg='AVG',
    # Login credentials for https://serdif-example.adaptcentre.ie/
    username='',
    password='',
    # Select the returning data format as 'CSV' or 'RDF'
    dataFormat='CSV'
)
print(exampleData)
```

### 4. Run your new file within the virtual environment of the project
* Open a terminal in the project folder
* Activate the virtual environment: `source venv/bin/activate`
* Run your file: `python your_file.py`
