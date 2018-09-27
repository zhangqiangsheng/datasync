#!/bin/bash
cd target
ftp -p -niv <<- EOF
open 172.21.205.30
user admin dongge
bin
cd jiege
delete pump-1.0.0.tar.gz
put pump-1.0.0.tar.gz
bye
EOF
echo "put ftp server successfull........."
