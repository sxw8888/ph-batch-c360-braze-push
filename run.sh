
#!/bin/bash

TABLE="DATASCIENCE.DSC.MAB_RUN_3"
COLUMNS="EXTERNAL_ID,OFFER_E"
WHERE_FILE=`cat error_2.txt`
START="0"

echo "Running... node --max-old-space-size=4096 --require dotenv/config  src/sync-home-store-update.js --table=$TABLE --columns=$COLUMNS --where=$WHERE_FILE --start=$START"
node --max-old-space-size=4096 --require dotenv/config  src/sync-home-store-update.js --table=$TABLE --columns=$COLUMNS --where=$WHERE_FILE --start=$START
