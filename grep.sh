
#!/bin/bash

cat nohup.out* | grep 'error inserting into braze'|  grep -oh '\"[0-9]*\"' | sed 's/"//g' | sed '/^$/d' | wc -l
cat nohup.out* | grep 'error inserting into braze'|  grep -oh '\"[0-9]*\"' | sed 's/"//g' | sed '/^$/d' > error.txt
