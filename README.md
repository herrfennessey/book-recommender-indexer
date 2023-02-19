# book-recommender-indexer

This is a pubsub subscriber that listens to the topics produced by our scrapers. If the item doesn't exist in the
database, it will be added. 

## Setup

1. Create a virtual environment and activate it
2. Install the requirements: `pip install -r requirements.txt`
3. Run FastApi with the command `uvicorn main:app --reload`

## Testing

1. Run the tests with the command `pytest`

## Deployment

1. This service is deployed to Cloud Run with a build trigger on the main branch.