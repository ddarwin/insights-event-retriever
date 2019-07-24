/**
 * Created by JE 2017-11-14
 *  the first query is used to determine the number of events that would be returned
 *  it's identical to the second query, except that we just want a count
 *  the second query is where the events are retrieved after the time interval has been fine tuned
 *  IDEA: instead of "guessing" on the time range, be more exact by knowing the exact counts
 *  to do that use timeseries when doing the counts - can go up to 6 hours with a 1-minute interval (nuance with this)
 *  if there are more than 1000 events in a minute, then redo count query for 5 minutes using a 1-second interval
 *  if there are more than 1000 events in a second, then you're SOL
 */

var $http = require ('request');
var assert = require ('assert');
var fs = require('fs');

/** UPDATE THESE variables **/
var INSIGHTS_QUERY_KEY = 'REPLACE_THIS_WITH_INSIGHTS_QUERY_KEY'; // Demotron
var accountID = '<REPLACE_THIS_WITH_NR_RPM_ID>';

var fileNamePrefix = "EXAMPLE"; //prefix to the name of output tsv - <prefix>-EVENTS_FROM_<startDate>_TO_<endDate>_CREATED_<created timestamp>.tsv

var fromWhere = "FROM Transaction WHERE appName = 'Tower-Miami'";
var firstQuery = "SELECT count(*) " + fromWhere; // don't change
var attributes = "timestamp, duration, httpResponseCode, name, tripId, error";
var secondQuery = "SELECT " + attributes + " " + fromWhere + " LIMIT 1000"; // don't change

var startDate = new Date("2018-01-03T00:00:00-08:00"); // set the start report date/time - remember to account for DST 11/2/2017
var   endDate = new Date("2018-01-04T00:00:00-08:00"); // set the end report date/time


/** END UPDATES, NOTHING BELOW SHOULD NEED TO BE CHANGED **/


var insights_url = 'https://insights-api.newrelic.com/v1/accounts/' + accountID + '/query';

var startTime = startDate.getTime();
var targetEndTime = endDate.getTime();

var timeIncrement = 21600000; // (milliseconds) - 6 hours by default, but may also use 5 minutes (this is used for retrieving the event counts)

var endTime = startTime + timeIncrement;

const tsBuckets1min = "1 minute";
const tsBuckets1sec = "1 second";
var timeIntervals = []; // each timestamp that's added to the array represents up to a 1000 event count
var firstTimeInterval = []; // first array element in the timeIntervals array [count, timestamp]
var priorStartTime = '';

var firstQueryCount = 0;
var optimizeCount = 0;

var firstQueryRetry = 0;
var secondQueryRetry = 0;
var retriedQueries = 0;
var mismatchCount = 0;

var loops = 1;
var totalEventCount = 0;
var reportRunStartTime = new Date();

var rawEventsFileName = 'output/' + fileNamePrefix + '-EVENTS_FROM_' + startDate.toISOString().slice(0,19) + '_TO_' + endDate.toISOString().slice(0,19) + '_CREATED_' + new Date().toISOString().slice(0,19) + '.tsv';

const DEBUG = false;

var debug = function(msg) {
    if (!DEBUG){
        return;
    }
    console.log("DEBUG: " + msg);
};


function retrieveAndSave(events) {

  if (events.length == firstTimeInterval[0]) console.log("\tcounted " + events.length + " events - matches initial count");
  else {
    console.log("\tcounted " + events.length + " events - MISMATCH with initial count");
    mismatchCount++;
  }

  debug("\n\tfirst event timestamp: " + events[0].timestamp);
  events.reverse(); // events are retrieved in descending order of timestamp, making them ascending
  debug("\tafter reversing, first timestamp: " + events[0].timestamp);

  var outTSV = "";
  // loop through all the events, extracting the values into a tab-delimited BIG string (TSV)
  for (i = 0; i < events.length; i++) {

      var event = events[i];

      for (key in event) outTSV = outTSV + event[key] + "\t";

      outTSV = outTSV + "\n";

  } // end outer loop

  debug("\nBIG STRING:\n");
  debug(outTSV);
  totalEventCount = totalEventCount + events.length;

  fs.appendFileSync(rawEventsFileName, outTSV);

}


function checkIncrement() {

  var timeDiff = endTime - startTime;

  if (timeDiff < 60000) return;

  // have to check if our time range is to a whole minute
  // if not, adjust so it's an exact minute to workaround the insights nuance (bug?) of rounding up the beginTime of the query
  if ( (timeDiff % 60000) != 0) {

    var newTimeIncrement = Math.floor(timeDiff/60000);
    console.log("\n\tNOTE: adjusting time increment to: " + newTimeIncrement + " min\n");
    endTime = startTime + (newTimeIncrement * 60000);

  }

}

/*
 * Determine number of events in 1-minute buckets for the specified query time range
 * If any 1-minute bucket has over 1000, take a second count using a 1-second bucket over the next 5 minutes
 * Also checks for non 200 responses (e.g. 503 are rate limiting), will retry up to 3 times if this occurs
 */
function getEventCounts(tsBuckets) {

  if (startTime == targetEndTime) { console.log("\n=> DONE - RETRIEVED " + totalEventCount + " TOTAL EVENTS in " + ((new Date() - reportRunStartTime)/1000/60).toFixed(1) + " (min) and " + retriedQueries + " retried queries and " + mismatchCount + " mismatch count\n"); return; }
  if (endTime > targetEndTime) endTime = targetEndTime;
  if (endTime - startTime < 60000) tsBuckets = tsBuckets1sec; //edge case, if under a minute time interval make sure buckets are 1 second

  checkIncrement();

  var firstQuerySince = ' SINCE ' + startTime + ' UNTIL ' + endTime + ' TIMESERIES ' + tsBuckets;
  var firstQueryComplete = firstQuery + firstQuerySince;

  var insightsOpts = {
      uri: insights_url,
      headers: {'Accept': 'application/json', 'X-Query-Key': INSIGHTS_QUERY_KEY},
      qs: {'nrql': firstQueryComplete}
  };
  debug("insights request: " + JSON.stringify(insightsOpts));
  if (optimizeCount == 0 && firstQueryRetry == 0) console.log("\n" + loops++ + ") retrieved " + totalEventCount + " events so far\n   optimizing time ranges from " + new Date(startTime).toLocaleString() + "\n\t" +  optimizeCount + ": " + firstQuerySince);
  else console.log("\t" +  optimizeCount + ": " + firstQuerySince);
  $http.get(insightsOpts, function(error, response, body) {

      debug("insights response body: " + body);
      debug("\treceived " + response.statusCode + " response from insights");

      if (response.statusCode != 200) {
        firstQueryRetry++;
        retriedQueries++;
        assert.ok(firstQueryRetry != 3, 'ERROR: received three consecutive non 200 responses in a row - giving up')
        console.log("\tERROR: received " + response.statusCode + " response code - retrying");
        getEventCounts(tsBuckets);
      }
      else {
        assert.ifError(error);
        //assert.equal(response.statusCode, 200, 'ERROR: did not receive 200 response calling insights'); // don't need this any more

        var json = JSON.parse(body);
        assert.ok(json.timeSeries, 'did not receive results');

        firstQueryRetry = 0; // reset back to zero as if we're here response was good
        var totalCount = json.total.results[0].count;
        var countTimeInterval = (endTime - startTime)/60000;
        console.log("\tcounted " + totalCount + " events in this " + countTimeInterval + " min time interval");

        // check for edge case where total events is less than 1000 - this will happen in the final iteration
        if (totalCount < 1001) {

          firstTimeInterval[0] = totalCount; // store that last count
          getEvents();
        }

        else {

          var bucketSize = json.metadata.bucketSizeMillis;
          var tooManyEvents = false;
          var runningCount = 0;
          var lastEndTime = '';

          if (optimizeCount == 0) priorStartTime = startTime; // save start time in case we have to requery into smaller buckets

          //build time intervals array
          for (var i = 0; i < json.timeSeries.length; i++) {

//console.log(i + ") count: " + json.timeSeries[i].results[0].count + " endTimeSeconds: " + json.timeSeries[i].endTimeSeconds);
            var currentCount = json.timeSeries[i].results[0].count;
            var currentEndTime = json.timeSeries[i].endTimeSeconds;

            // if not more than 1000 add to the running count until it reaches 1000
            if (currentCount < 1001) {

              // keep track of running count and save last end time
              if (currentCount + runningCount < 1001) {
                runningCount = runningCount + currentCount;
                lastEndTime = currentEndTime;
              }

              // add the last end time to our time interval array, reset count and save last end time
              else {
                console.log("\tPUSHING: "  + runningCount + " lastEndTime: " + lastEndTime);
                var timeIntervalEntry = [runningCount, lastEndTime];
                timeIntervals.push(timeIntervalEntry);

                runningCount = currentCount;
                lastEndTime = currentEndTime;
              }
            }
          // if more than 1000 then need to requery with 1 sec interval unless this is already a 1 sec interval
          // also need to save the prior endTime if we have a running count
          // if we have time intervals, then start time is from lastEndTime, otherwise use original start time
            else {

              tooManyEvents = true;
              if (bucketSize == 1000) assert(false, "ERROR: more than 1000 events in a second - exiting");
              break;

            }

          } // end for

          if (!tooManyEvents) {

            // at this point we have an array of time intervals, use the first entry as the end time to get the events
            firstTimeInterval = timeIntervals.shift();
            endTime = firstTimeInterval[1] * 1000;

            if (optimizeCount == 1) startTime = priorStartTime; // set the start time back to the original start time for this cycle
            getEvents();

          }

          else {

            optimizeCount++;
            // if we have time intervals then start time is from last end time in array
            if (timeIntervals.length > 0) {
              startTime = timeIntervals[timeIntervals.length-1][1]*1000;
              endTime = startTime + (5 * 60 * 1000); // for the 1 sec buckets our query time range is only 5 minutes
              getEventCounts(tsBuckets1sec);
            }

            else {
              endTime = startTime + (5 * 60 * 1000);
              getEventCounts(tsBuckets1sec);
            }

          }
        }
      }

  }); // end http get

}

function getEvents() {

  var secondQuerySince = ' SINCE ' + startTime + ' UNTIL ' + endTime;
  var secondQueryComplete = secondQuery + secondQuerySince;

  var insightsOpts = {
      uri: insights_url,
      headers: {'Accept': 'application/json', 'X-Query-Key': INSIGHTS_QUERY_KEY},
      qs: {'nrql': secondQueryComplete}
  };
  debug("insights request: " + JSON.stringify(insightsOpts));
  if (secondQueryRetry == 0) console.log("   retrieving events until " + new Date(endTime).toLocaleString() + "\t" + secondQuerySince);
  else console.log("\t" + secondQuerySince);
  $http.get(insightsOpts, function(error, response, body) {

      debug("insights response body: " + body);
      debug("\treceived " + response.statusCode + " response from insights");

      if (response.statusCode != 200) {
        secondQueryRetry++;
        retriedQueries++;
        assert.ok(secondQueryRetry != 3, 'ERROR: received three consecutive non 200 responses in a row - giving up')
        console.log("\tERROR: received " + response.statusCode + " response code - retrying");
        getEvents();
      }
      else {
        assert.ifError(error);
        //assert.equal(response.statusCode, 200, 'ERROR: did not receive 200 response calling insights'); // don't need this assert any longer

        var json = JSON.parse(body);
        assert.ok(json.results, 'did not receive results');

        secondQueryRetry = 0; // reset back to zero as if we're here response was good

        retrieveAndSave(json.results[0].events);

        startTime = endTime; // new startTime is last end time

        if (timeIntervals.length > 0) {
          firstTimeInterval = timeIntervals.shift();
          endTime = firstTimeInterval[1] * 1000;
          getEvents();

        }

        else {

          endTime = startTime + timeIncrement;
          if (endTime > targetEndTime) endTime = targetEndTime;
          optimizeCount = 0;
          getEventCounts(tsBuckets1min);

        }

      }

  }); // end http get

}


function startHere() {

  console.log("\n=> BASE QUERY FOR EVENT COUNTS:\n\n" + firstQuery);
  console.log("\n=> BASE QUERY FOR EVENTS:\n\n" + secondQuery + "\n");

  var header = attributes.replace(/,/g, "\t") + "\n";
  fs.writeFileSync(rawEventsFileName, header);
  getEventCounts(tsBuckets1min);

}

startHere();
