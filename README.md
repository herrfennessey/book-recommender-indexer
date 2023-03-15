# book-recommender-indexer

This is where the business logic of the scraping infrastructure lives. It will receive messages on multiple pubsub
topics, such as users to crawl, books to crawl, etc. It has debouncing logic built in to where it will try and avoid
redundant operations, such as crawling a book we already have a copy of.

Currently, the following topics are "pushing" to our cloud run indexer to the various endpoints, because cloud run can't use pub/sub like a grown up

* `scraper-book-v1`
* `scraper-user-review-v1`
* `scraper-profile-v1`

## Setup

1. Create a virtual environment and activate it
2. Install the requirements: `pip install -r requirements.txt`
3. Run FastApi with the command `uvicorn main:app --reload`

## Testing

1. Run the tests with the command `pytest`

## Deployment

1. This service is deployed to Cloud Run with a build trigger on the main branch.
