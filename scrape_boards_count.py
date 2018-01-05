""" Core scraper for bitcointalk.org. """
import bitcointalk
import logging
import memoizer
import os
import sys
import traceback

boardId = 159

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p')

# Make sure we don't rescrape information already in the DB
memoizer.remember()

ignore = [2284373, 718124, 2559282, 1042324, 936724, 780979]

logging.info("Beginning scrape of board ID...".format(boardId))
board = memoizer.scrapeBoard(boardId)
logging.info("Found {0} topic pages in board...".format(
    board['num_pages']))
for boardPageNum in range(1, board['num_pages'] + 1):
    logging.info(">Scraping page {0}...".format(boardPageNum))
    topicIds = memoizer.scrapeTopicIds(boardId, boardPageNum)
    for topicId in topicIds:
        if topicId in ignore:
            print "Ignoring {0}".format(topicId)
            continue
        logging.info(">>Starting scrape of topic ID {0}...".format(topicId))
        try:
            topic = memoizer.scrapeTopic(topicId)
        except Exception as e:
            print '-'*60
            print "Could not request URL for topic {0}:".format(topicId)
            print traceback.format_exc()
            print '-'*60
            logging.info(">>Could not request URL for topic {0}:".format(
                topicId))
            continue

logging.info("All done.")
logging.info("Made {0} requests in total.".format(bitcointalk.countRequested))
