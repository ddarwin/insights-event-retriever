## insights-events-retriever
node script that given a start/end time and a query will produce a tab-delimited file with the all the insights events matching the query over the specific time frame.

**CAVEAT:  there cannot be more than 1000 events in any 1 sec period**

### Download the node  modules

In your workspace run the command
    
    myworkspaceDir>  npm install
    
NPM will look at the dependencies in the package.json and automatically download the modules into the `.\node_modules` directory. 

For more details see docs [here](https://docs.npmjs.com/getting-started/using-a-package.json).
 
### Set NODE_PATH 
Set your NODE_PATH, this tells node where to find the modules that are imported by your app.

    myworkspaceDir>  export NODE_PATH=/Users/jdoe/myworkspaceDir/insights-events-retriever
    
### Configuration
The script contains a few variables that require updating. Please update the following within getEvents.js
```
INSIGHTS_QUERY_KEY
accountID
fileNamePrefix
fromWhere
attributes
startDate
endDate 
```
### Running the script
    myworkspaceDir>  node getEvents.js 
